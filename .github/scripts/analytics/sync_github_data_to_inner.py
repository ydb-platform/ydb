#!/usr/bin/env python3
"""Copy GitHub tables to internal YDB using the existing analytics helpers."""

import os

import ydb
from data_mart_executor import create_table, ydb_type_to_str
from ydb_wrapper import YDBWrapper


TABLES = (("issues", "issue_number"), ("pull_requests", "pr_number"))


def sync(source, destination):
    for table, number_column in TABLES:
        source_path = source.get_table_path(table)
        target_path = "github_data/" + table
        # Full scans bootstrap old reviews and propagate removed labels/closed states.
        rows, column_types = source.execute_scan_query_with_metadata(
            f"SELECT * FROM `{source_path}`;", query_name="sync_" + table)
        create_table(destination, target_path, column_types, "column", ["created_date"],
                     ["created_date", number_column, "project_item_id"], None, None)
        types = ydb.BulkUpsertColumns()
        for name, column_type in column_types:
            types.add_column(name, ydb_type_to_str(column_type, "COLUMN")[0])
        destination.bulk_upsert_batches(target_path, rows, types, batch_size=1000,
                                        query_name="sync_" + table)


def main():
    # Separate credentials: cloud service account for reads, internal token for writes.
    for name in ("YDB_INNER_TOKEN", "YDB_INNER_ENDPOINT", "YDB_INNER_DATABASE"):
        if not os.environ.get(name):
            raise RuntimeError(f"{name} is required")
    token = os.environ["YDB_INNER_TOKEN"]
    config = {"databases": {"main": {
        "endpoint": os.environ["YDB_INNER_ENDPOINT"],
        "path": os.environ["YDB_INNER_DATABASE"],
    }}}
    with YDBWrapper() as source, YDBWrapper(
        config=config, credentials=ydb.AccessTokenCredentials(token), enable_statistics=False,
    ) as destination:
        if source.database_path.rstrip("/") == destination.database_path.rstrip("/"):
            raise ValueError("Source and destination databases must differ")
        sync(source, destination)


if __name__ == "__main__":
    main()
