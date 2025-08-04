"""Utility for loading NetSuite table configuration from JSON.

This script demonstrates reading a JSON file with NetSuite table
metadata and printing integration name and primary keys for each
entry.  The JSON file is expected to reside in the same directory as
this script and to be named `netsuite_tables.json`.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List

CONFIG_FILE = Path(__file__).with_name("netsuite_tables.json")


def load_config(config_path: Path = CONFIG_FILE) -> Dict[str, Dict[str, str]]:
    """Load NetSuite table configuration from ``config_path``.

    Parameters
    ----------
    config_path: Path
        Location of the JSON configuration file.

    Returns
    -------
    dict
        Mapping of table names to their integration and primary key
        details.
    """
    with config_path.open() as fh:
        return json.load(fh)


def parse_primary_keys(key_str: str) -> List[str]:
    """Split a comma-delimited string of primary keys into a list."""
    return [k.strip() for k in key_str.split(",") if k.strip()]


if __name__ == "__main__":
    config = load_config()
    for table_name, info in config.items():
        integration = info.get("integration")
        primary_keys = parse_primary_keys(info.get("primary_keys", ""))
        print(f"{table_name}: integration={integration}, primary_keys={primary_keys}")
