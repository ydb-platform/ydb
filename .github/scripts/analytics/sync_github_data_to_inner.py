#!/usr/bin/env python3
"""Copy the GitHub dashboard projection from cloud YDB to internal YDB.

Read the complete source on every run: an initial sync must include old open
reviews, and later syncs must propagate removed labels and closed/merged states.
No GitHub API calls or age/label/state filters are applied here.
"""

import argparse
import os
from datetime import datetime, timezone


COMMON_COLUMNS = (
    ("url", "Utf8"),
    ("title", "Utf8"),
    ("state", "Utf8"),
    ("repository_name", "Utf8"),
    ("repository_url", "Utf8"),
    ("labels", "Json"),
    ("created_at", "Timestamp"),
    ("updated_at", "Timestamp"),
    ("closed_at", "Timestamp"),
    ("exported_at", "Timestamp"),
)
NUMBER_COLUMNS = {"issues": "issue_number", "pull_requests": "pr_number"}


def columns_for(table):
    return ((NUMBER_COLUMNS[table], "Uint64"),) + COMMON_COLUMNS


def source_query(table_path, table):
    if "`" in table_path:
        raise ValueError("Invalid source table path")
    columns = ", ".join("`%s`" % name for name, _ in columns_for(table))
    return "SELECT %s FROM `%s`;" % (columns, table_path)


def prepare_rows(rows, synced_at):
    """Collapse project memberships to one object per URL, newest export wins."""
    objects = {}
    for row in rows:
        row = dict(row)
        url = row.get("url")
        if not url:
            raise ValueError("Source row has no GitHub URL")
        previous = objects.get(url)
        # YDBWrapper returns timestamps as microseconds since the epoch.
        if previous is None or (row.get("exported_at") or 0) >= (previous.get("exported_at") or 0):
            objects[url] = row
    for row in objects.values():
        row["synced_at"] = synced_at
    return list(objects.values())


def create_table(pool, table_path, table):
    if "`" in table_path:
        raise ValueError("Invalid destination table path")
    # Row tables work on the internal cluster and are sufficient for this small
    # projection. Use URL as the key so project membership cannot duplicate counts.
    columns = columns_for(table) + (("synced_at", "Timestamp"),)
    definitions = ", ".join(
        "`%s` %s%s" % (name, kind, " NOT NULL" if name == "url" else "")
        for name, kind in columns
    )
    query = "CREATE TABLE IF NOT EXISTS `%s` (%s, PRIMARY KEY (`url`));" % (table_path, definitions)
    pool.retry_operation_sync(lambda session: session.execute_scheme(query))


def sync_table(source, driver, pool, database, table, batch_size):
    import ydb

    source_path = source.get_table_path(table)
    rows = source.execute_scan_query(source_query(source_path, table), query_name="github_inner_sync_" + table)
    synced_at = datetime.now(timezone.utc)
    rows = prepare_rows(rows, synced_at)
    table_path = database.rstrip("/") + "/github_data/" + table
    create_table(pool, table_path, table)
    types = ydb.BulkUpsertColumns()
    for name, kind in columns_for(table) + (("synced_at", "Timestamp"),):
        primitive = getattr(ydb.PrimitiveType, kind)
        types.add_column(name, primitive if name == "url" else ydb.OptionalType(primitive))
    for offset in range(0, len(rows), batch_size):
        batch = rows[offset:offset + batch_size]
        ydb.retry_operation_sync(lambda batch=batch: driver.table_client.bulk_upsert(table_path, batch, types))
    print("Synced %d objects to %s" % (len(rows), table_path))
    return len(rows)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", default=os.environ.get("YDB_INNER_ENDPOINT", "grpc://vault-klg.ydb.yandex.net:2135"))
    parser.add_argument("--database", default=os.environ.get("YDB_INNER_DATABASE", "/vault-klg/kikimr/prod/ydb-qa-db-inner"))
    parser.add_argument("--batch-size", type=int, default=100)
    args = parser.parse_args(argv)
    if args.batch_size < 1:
        parser.error("--batch-size must be positive")
    token = os.environ.get("YDB_INNER_TOKEN")
    if not token:
        parser.error("YDB_INNER_TOKEN is required (configure the GitHub Actions secret)")

    import ydb
    from ydb_wrapper import YDBWrapper

    # Credentials are explicit for the destination. The cloud service account
    # remains in the environment exclusively for the source wrapper.
    with YDBWrapper() as source:
        if not source.check_credentials():
            return 1
        if source.database_path.rstrip("/") == args.database.rstrip("/"):
            parser.error("Source and destination databases must differ")
        with ydb.Driver(endpoint=args.endpoint, database=args.database,
                        credentials=ydb.AccessTokenCredentials(token)) as driver:
            driver.wait(timeout=60, fail_fast=True)
            directory = args.database.rstrip("/") + "/github_data"
            try:
                ydb.retry_operation_sync(lambda: driver.scheme_client.make_directory(directory))
            except ydb.SchemeError:
                # Another run may already have created the directory. Verify it
                # exists instead of treating any schema error as success.
                driver.scheme_client.describe_path(directory)
            with ydb.SessionPool(driver) as pool:
                for table in NUMBER_COLUMNS:
                    sync_table(source, driver, pool, args.database, table, args.batch_size)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
