"""Utility to perform soft deletes between NetSuite and Hudi tables.

This script reads records from a NetSuite source table and a corresponding
Hudi table, marks missing NetSuite rows as deleted in the Hudi table and logs
summary information to DynamoDB. The implementation has been refactored to
avoid global side effects and to improve error handling.
"""

from __future__ import annotations

import sys
import time
from datetime import datetime
from typing import Dict

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, when

from utils import report_error
from validation_utils import get_secret_for_netsuite

# ---------------------------------------------------------------------------
# Global Hudi options that remain constant across invocations
# ---------------------------------------------------------------------------
BASE_HUDI_OPTIONS: Dict[str, str] = {
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.operation": "upsert",
    "hoodie.datasource.write.hive_style_partitioning": "true",
    "hoodie.datasource.write.reconcile.schema": "false",
    "hoodie.parquet.compression.codec": "gzip",
    "hoodie.datasource.hive_sync.enable": "true",
    "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
    "hoodie.datasource.hive_sync.use_jdbc": "false",
    "hoodie.datasource.hive_sync.mode": "hms",
    "hoodie.write.concurrency.mode": "single_writer",
    "hoodie.clean.automatic": "true",
    "hoodie.clean.async": "true",
    "hoodie.cleaner.policy": "KEEP_LATEST_COMMITS",
    "hoodie.cleaner.commits.retained": 15,
    "hoodie.cleaner.policy.failed.writes": "EAGER",
    "hoodie.bulkinsert.shuffle.parallelism": 2000,
    "hoodie.simple.index.update.partition.path": "false",
    "hoodie.write.set.null.for.missing.columns": "true",
    "hoodie.metadata.enable": "false",
    "hoodie.datasource.write.partitionpath.field": "partition_key",
    "hoodie.datasource.hive_sync.partition_fields": "partition_key",
}

HUDI_TABLE_PRECOMBINE = {
    "netsuite_transaction_lines": "date_last_modified_gmt",
    "netsuite_transaction_links": "date_last_modified",
    "netsuite_nexttransactionlinelink": "_sdc_sequence",
    "netsuite_transactionline": "_sdc_sequence",
}
class SoftDeleteProcessor:
    """Encapsulates NetSuite-to-Hudi soft delete logic for reuse."""

    def __init__(self, spark: SparkSession, logger, args: Dict[str, str]):
        self.spark = spark
        self.logger = logger
        self.args = args
        self.environment = args["JOB_ENV"]
        self.netsuite_version = args["NETSUITE_VERSION"]
        self.primary_keys = args["PRIMARY_KEYS"]
        self.netsuite_table = args["NETSUITE_TABLE"]
        self.integration_source_system = args["INTEGRATION_SOURCE_SYSTEM"]
        self.hudi_table = f"{self.integration_source_system}_{self.netsuite_table}"
        self.primary_key_list = [k.strip() for k in self.primary_keys.split(",")]
        self.secret_values = get_secret_for_netsuite(
            args["NETSUITE_1_DB_SECRET_NAME"], logger
        )

    # ------------------------------------------------------------------
    # Data retrieval helpers
    # ------------------------------------------------------------------
    def _get_netsuite_source_table_data(self, hudi_count: int) -> DataFrame:
        """Fetch primary key data from the NetSuite source table."""
        try:
            jdbc_options: Dict[str, str] = {"fetchsize": "10000"}
            if hudi_count > 1_000_000:
                jdbc_options.update(
                    {
                        "partitionColumn": self.primary_key_list[0],
                        "lowerBound": "1",
                        "upperBound": str(hudi_count),
                        "numPartitions": "10",
                    }
                )

            query = f"SELECT {self.primary_keys} FROM {self.netsuite_table}"
            self.logger.info(f"netsuite query: {query}")

            if self.netsuite_version == "netsuite_1":
                jdbc_options.update(
                    {
                        "url": self.secret_values["connectionUrl"],
                        "dbtable": f"({query}) as subq",
                        "driver": "com.netsuite.jdbc.openaccess.OpenAccessDriver",
                        "user": self.secret_values["username"],
                        "password": self.secret_values["password"],
                    }
                )
            elif self.netsuite_version == "netsuite_2":
                jdbc_options.update(
                    {
                        "url": self.secret_values["db_url"],
                        "driver": "netsuite.driver.Driver",
                        "account_id": self.secret_values["account_id"],
                        "consumer_key": self.secret_values["consumer_key"],
                        "consumer_secret": self.secret_values["consumer_secret"],
                        "token_id": self.secret_values["token_id"],
                        "token_secret": self.secret_values["token_secret"],
                        "dbtable": f"({query}) as subq",
                    }
                )

            return self.spark.read.format("jdbc").options(**jdbc_options).load()
        except Exception as exc:  # pragma: no cover - log and rethrow
            self.logger.error(
                f"Error fetching data from NetSuite table {self.netsuite_table}: {exc}"
            )
            raise RuntimeError(str(exc)) from exc

    def _athena_table_exists(self, client, database: str, table: str) -> bool:
        """Return True if the given table exists in Athena."""
        try:
            client.get_table_metadata(
                CatalogName="AwsDataCatalog",
                DatabaseName=database,
                TableName=table,
            )
            return True
        except (client.exceptions.MetadataException, client.exceptions.InvalidRequestException):
            return False

    def _get_hudi_table_data(self) -> DataFrame:
        """Read active records from the Hudi table via Athena."""
        try:
            client = boto3.client("athena")
            hudi_database = f"{self.environment}_hudi_db"

            if not self._athena_table_exists(client, hudi_database, self.hudi_table):
                raise Exception(
                    f"Athena table {hudi_database}.{self.hudi_table} does not exist"
                )

            querystring = (
                f"SELECT * FROM {hudi_database}.{self.hudi_table} where is_deleted <> '1'"
            )
            self.logger.info(f"query : {querystring}")

            response = client.start_query_execution(
                QueryString=querystring,
                QueryExecutionContext={"Database": hudi_database},
                ResultConfiguration={"OutputLocation": self.args["s3_output_location"]},
            )

            query_execution_id = response["QueryExecutionId"]

            while True:
                status_response = client.get_query_execution(
                    QueryExecutionId=query_execution_id
                )
                status = status_response["QueryExecution"]["Status"]["State"]
                if status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
                    break
                time.sleep(2)

            if status != "SUCCEEDED":
                self.logger.error(
                    f"Athena query failed with status: {status}, more context {status_response['QueryExecution']['Status']}"
                )
                raise Exception(f"Athena query failed with status: {status}")

            output_location = status_response["QueryExecution"]["ResultConfiguration"][
                "OutputLocation"
            ]
            self.logger.info(f"query result output location : {output_location}")

            return self.spark.read.csv(output_location, header=True, inferSchema=True)
        except Exception as exc:  # pragma: no cover - log and rethrow
            self.logger.error(
                f"Error getting data from Glue Catalog for {self.hudi_table}: {exc}"
            )
            raise Exception(str(exc))

    def _write_to_hudi_table(self, df: DataFrame) -> None:
        """Write a DataFrame to a Hudi table with the configured options."""
        try:
            precombine_field = HUDI_TABLE_PRECOMBINE.get(self.hudi_table)
            if not precombine_field:
                raise ValueError(f"Unsupported hudi table {self.hudi_table}")

            hudi_options = BASE_HUDI_OPTIONS.copy()
            hudi_options.update(
                {
                    "hoodie.table.name": self.hudi_table,
                    "hoodie.datasource.write.recordkey.field": self.primary_keys,
                    "hoodie.datasource.write.precombine.field": precombine_field,
                    "hoodie.datasource.hive_sync.table": self.hudi_table,
                    "hoodie.payload.ordering.field": precombine_field,
                    "hoodie.datasource.hive_sync.database": f"{self.environment}_hudi_db",
                }
            )

            output_path = (
                f"s3://data-warehouse-{self.environment}-hudi-tables/"
                f"{self.integration_source_system}/{self.netsuite_table}/"
            )
            self.logger.info(f"Hudi options are: {hudi_options}")
            self.logger.info(f"Writing data to Hudi table: {output_path}")

            df_string = df.select([col(c).cast("string").alias(c) for c in df.columns])
            self.logger.debug(f"Schema before writing {df_string.schema}")
            df_string.write.format("hudi").options(**hudi_options).mode("append").save(
                output_path
            )
        except Exception as exc:  # pragma: no cover - log and rethrow
            self.logger.error(f"Error occurred while writing data to Hudi table: {exc}")
            raise Exception(str(exc))

    def _log_dynamodb(self, item: Dict[str, str]) -> None:
        """Persist summary information to DynamoDB."""
        try:
            dynamodb = boto3.resource("dynamodb")
            dynamodb_table = dynamodb.Table(
                self.args["soft_del_job_dynamodb_table_name"]
            )
            dynamodb_table.put_item(Item=item)
            self.logger.info("record inserted into dynamodb")
        except Exception as exc:  # pragma: no cover - log and rethrow
            self.logger.error(f"Error in log_dynamodb_soft_deletions: {exc}")
            raise Exception(str(exc))

    def _validate_column_count(self, existing_df: DataFrame, new_df: DataFrame) -> None:
        """Ensure the update DataFrame matches the existing Hudi table's column count."""
        existing_cols = existing_df.columns
        new_cols = new_df.columns
        if len(existing_cols) != len(new_cols):
            extra = set(new_cols) - set(existing_cols)
            missing = set(existing_cols) - set(new_cols)
            msg = (
                "Column mismatch between existing Hudi table "
                f"({len(existing_cols)} columns) and update DataFrame "
                f"({len(new_cols)} columns). "
                f"Missing columns: {sorted(missing)}; Extra columns: {sorted(extra)}"
            )
            self.logger.error(msg)
            raise ValueError(msg)

    def _validate_schema(self, existing_df: DataFrame, new_df: DataFrame) -> None:
        """Ensure column names and data types match between DataFrames."""
        existing_schema = {
            field.name: field.dataType.simpleString() for field in existing_df.schema.fields
        }
        new_schema = {
            field.name: field.dataType.simpleString() for field in new_df.schema.fields
        }

        mismatches = []
        for col, dtype in existing_schema.items():
            new_dtype = new_schema.get(col)
            if new_dtype is None:
                mismatches.append(f"missing column {col}")
            elif new_dtype != dtype:
                mismatches.append(
                    f"{col} type mismatch (expected {dtype}, got {new_dtype})"
                )

        extra_cols = [c for c in new_schema if c not in existing_schema]
        mismatches.extend([f"extra column {c}" for c in extra_cols])

        if mismatches:
            msg = (
                "Schema mismatch between existing Hudi table and update DataFrame: "
                + "; ".join(sorted(mismatches))
            )
            self.logger.error(msg)
            raise ValueError(msg)

    # ------------------------------------------------------------------
    # Main workflow
    # ------------------------------------------------------------------
    def run(self) -> None:
        """Execute the soft delete process."""
        try:
            self.logger.info(
                "--------------------*** Starting Netsuite HUDI Table Soft Delete Process ***--------------------"
            )

            formatted_utc = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            self.args["timestamp"] = formatted_utc

            hudi_df = self._get_hudi_table_data()
            hudi_df_count = hudi_df.count()
            self.logger.info(f"hudi_df_count: {hudi_df_count}")

            netsuite_df = self._get_netsuite_source_table_data(hudi_df_count)
            netsuite_df_count = netsuite_df.count()
            self.logger.info(f"netsuite_df_count: {netsuite_df_count}")

            count_diff = netsuite_df_count - hudi_df_count
            self.logger.info(f"netsuite_hudi_table_rec_count_diff: {count_diff}")

            if count_diff > 0:
                join_df = hudi_df.alias("hudi").join(
                    netsuite_df.alias("netsuite"),
                    on=self.primary_key_list,
                    how="left",
                ).withColumn(
                    "is_deleted_new",
                    when(
                        col(f"netsuite.{self.primary_key_list[0]}").isNull(),
                        lit("1"),
                    ).otherwise(col("hudi.is_deleted")),
                )

                df_diff_filter = (
                    join_df.filter(col("is_deleted_new") == "1")
                    .drop("is_deleted")
                    .withColumnRenamed("is_deleted_new", "is_deleted")
                )

                cols_to_drop = [c for c in df_diff_filter.columns if c.startswith("_hoodie_")]
                final_df = df_diff_filter.drop(*cols_to_drop)
                self._validate_column_count(hudi_df, final_df)
                self._validate_schema(hudi_df, final_df)
                self._write_to_hudi_table(final_df)

            item = {
                "soft_delete_key": self.netsuite_table + formatted_utc,
                "netsuite_table_name": self.netsuite_table,
                "hudi_table_name": self.hudi_table,
                "netsuite_table_count": netsuite_df_count,
                "hudi_table_count": hudi_df_count,
                "record_count_diff": count_diff,
                "status": "Completed",
                "last_run_ts": formatted_utc,
                "job_run_id": self.args["JOB_RUN_ID"],
            }
            self._log_dynamodb(item)
        except Exception as exc:  # pragma: no cover - log and rethrow
            self.logger.error(f"Error in process_soft_delete: {exc}")
            raise Exception(str(exc))


if __name__ == "__main__":
    try:
        spark = (
            SparkSession.builder.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.sql.hive.convertMetastoreParquet", "false")
            .config(
                "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog"
            )
            .config(
                "spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension"
            )
            .config("spark.sql.legacy.pathOptionBehavior.enabled", "true")
            .getOrCreate()
        )
        sc = spark.sparkContext
        glueContext = GlueContext(sc)
        logger = glueContext.get_logger()

        required_args = [
            "JOB_NAME",
            "JOB_ENV",
            "soft_del_job_dynamodb_table_name",
            "rollbar_access_token_secret",
            "rollbar_environment",
            "ERROR_REPORTER_MESSAGE_PREFIX",
            "NETSUITE_1_DB_SECRET_NAME",
            "NETSUITE_2_DB_SECRET_NAME",
            "INTEGRATION_SOURCE_SYSTEM",
            "NETSUITE_TABLE",
            "PRIMARY_KEYS",
            "NETSUITE_VERSION",
            "hudi_glue_config_bucket",
            "hudi_glue_config_file",
            "s3_output_location",
        ]
        args = getResolvedOptions(sys.argv, required_args)

        job = Job(glueContext)
        job.init(args["JOB_NAME"], args)

        processor = SoftDeleteProcessor(spark, logger, args)
        processor.run()
        job.commit()
    except Exception as exc:  # pragma: no cover - log and rethrow
        logger.error(f"Error in main: {exc}")
        report_error(
            rollbar_access_token_secret=args["rollbar_access_token_secret"],
            rollbar_environment=args["rollbar_environment"],
            message=str(exc),
            message_prefix=args["ERROR_REPORTER_MESSAGE_PREFIX"],
        )
        raise
