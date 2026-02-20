#!/usr/bin/env python3
"""
Sync area -> owner_team mapping from .github/config/owner_area_mapping.json to YDB.
Table: one row per area, join in SQL (e.g. github.sql) to get owner_team for issues by area.
"""

import json
import os
import sys
import ydb
from typing import List, Tuple
from ydb_wrapper import YDBWrapper


def load_area_to_owner(config_dir: str) -> List[Tuple[str, str]]:
    """Build (area, owner_team) from owner_area_mapping.json (owner -> area). Prefer lowercase owner when same area."""
    path = os.path.join(config_dir, 'owner_area_mapping.json')
    area_to_owner = {}
    with open(path, 'r', encoding='utf-8') as f:
        owner_to_area = json.load(f)
    for owner_team, area in owner_to_area.items():
        if not area:
            continue
        if area not in area_to_owner or (owner_team.islower() and not area_to_owner[area].islower()):
            area_to_owner[area] = owner_team
    return [(a, o) for a, o in area_to_owner.items()]


def create_table(ydb_wrapper, table_path: str):
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS `{table_path}` (
            `area` Utf8 NOT NULL,
            `owner_team` Utf8 NOT NULL,
            PRIMARY KEY (`area`)
        )
        WITH (STORE = COLUMN)
    """
    ydb_wrapper.create_table(table_path, create_sql)


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_dir = os.path.join(script_dir, '..', '..', 'config')
    rows = load_area_to_owner(config_dir)
    if not rows:
        print("No mapping rows loaded")
        return 1

    with YDBWrapper() as ydb_wrapper:
        if not ydb_wrapper.check_credentials():
            return 1
        table_path = ydb_wrapper.get_table_path("area_to_owner_mapping")
        create_table(ydb_wrapper, table_path)

        bulk_rows = [{'area': a, 'owner_team': o} for a, o in rows]
        col_types = (
            ydb.BulkUpsertColumns()
            .add_column("area", ydb.PrimitiveType.Utf8)
            .add_column("owner_team", ydb.PrimitiveType.Utf8)
        )
        ydb_wrapper.bulk_upsert(table_path, bulk_rows, col_types)
        print(f"Upserted {len(bulk_rows)} rows to {table_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
