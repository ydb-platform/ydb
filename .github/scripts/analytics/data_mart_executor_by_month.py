#!/usr/bin/env python3
"""
Run data_mart executor by month for the last N months to avoid long-running scan
and connection timeouts (e.g. Broken pipe / UNAVAILABLE).

Expects the SQL query to define $month_start and $month_end (Date) and to filter
by them (e.g. AND dt.d >= $month_start AND dt.d < $month_end). See github_issues_timeline.sql.
"""

import argparse
import re
import sys
import time
from datetime import date, timedelta

import ydb

from data_mart_executor import (
    create_table_if_not_exists,
    ydb_type_to_str,
)
from ydb_wrapper import YDBWrapper

import os

_dir = os.path.dirname(__file__)
repo_path = os.path.abspath(f"{_dir}/../../../")


def _first_day_of_next_month(d: date) -> date:
    if d.month == 12:
        return d.replace(year=d.year + 1, month=1, day=1)
    return d.replace(month=d.month + 1, day=1)


def get_month_ranges(months_back: int, oldest_first: bool = True):
    """Yield (month_start, month_end) for the last months_back months + current month. oldest_first=True: from oldest to newest."""
    current_month_start = date.today().replace(day=1)
    end = current_month_start
    ranges = []
    for _ in range(months_back):
        start = (end - timedelta(days=1)).replace(day=1)
        ranges.append((start, end))
        end = start
    # include current (active) month so far
    ranges.insert(0, (current_month_start, _first_day_of_next_month(current_month_start)))
    if oldest_first:
        ranges.reverse()
    for r in ranges:
        yield r


def substitute_month_bounds(sql: str, month_start: date, month_end: date) -> str:
    """Replace $month_start and $month_end declarations in the query."""
    sql = re.sub(
        r'\$month_start\s*=\s*Date\s*\([^)]+\)\s*;',
        f'$month_start = Date("{month_start.isoformat()}");',
        sql,
        count=1,
    )
    sql = re.sub(
        r'\$month_end\s*=\s*Date\s*\([^)]+\)\s*;',
        f'$month_end = Date("{month_end.isoformat()}");',
        sql,
        count=1,
    )
    return sql


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run data mart executor by month (last N months) with retry on connection errors."
    )
    parser.add_argument("--table_path", required=True, help="Table path and name")
    parser.add_argument("--query_path", required=True, help="Path to the SQL query file")
    parser.add_argument(
        "--store_type",
        choices=["column", "row"],
        required=True,
        help="Table store type (column or row)",
    )
    parser.add_argument(
        "--partition_keys",
        nargs="+",
        required=True,
        help="List of partition keys",
    )
    parser.add_argument(
        "--primary_keys",
        nargs="+",
        required=True,
        help="List of primary keys",
    )
    parser.add_argument("--ttl_min", type=int, help="TTL in minutes")
    parser.add_argument("--ttl_key", help="TTL key column name")
    parser.add_argument(
        "--by_month",
        type=int,
        default=12,
        metavar="N",
        help="Export last N months one by one (default: 12)",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=3,
        help="Max retries per month on connection error (default: 3)",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    table_path = args.table_path
    batch_size = 1000
    query_name = table_path.split("/")[-1]
    sql_query_path = os.path.join(repo_path, args.query_path)
    print(f"Query: {sql_query_path}, by_month={args.by_month}")

    with open(sql_query_path, "r") as f:
        base_sql = f.read()

    with YDBWrapper() as ydb_wrapper:
        if not ydb_wrapper.check_credentials():
            return 1

        column_types_map = None
        table_created = False

        for month_start, month_end in get_month_ranges(args.by_month):
            label = f"{month_start}..{month_end}"
            sql = substitute_month_bounds(base_sql, month_start, month_end)

            for attempt in range(1, args.retries + 1):
                try:
                    print(f"[{label}] Scan (attempt {attempt}/{args.retries}) ...")
                    results, column_types = ydb_wrapper.execute_scan_query_with_metadata(
                        sql, query_name
                    )
                    break
                except Exception as e:
                    err_str = str(e).lower()
                    is_connection_error = (
                        "connection" in err_str
                        or "unavailable" in err_str
                        or "broken pipe" in err_str
                        or type(e).__name__ == "ConnectionLost"
                    )
                    if is_connection_error:
                        if attempt < args.retries:
                            wait = 10
                            print(f"[{label}] Connection error, retry in {wait}s: {e}")
                            time.sleep(wait)
                        else:
                            print(f"[{label}] Failed after {args.retries} attempts: {e}", file=sys.stderr)
                            return 1
                    else:
                        raise

            if not results:
                print(f"[{label}] No rows, skip upsert")
                continue

            if not table_created:
                create_table_if_not_exists(
                    ydb_wrapper,
                    table_path,
                    column_types,
                    args.store_type,
                    args.partition_keys,
                    args.primary_keys,
                    args.ttl_min,
                    args.ttl_key,
                )
                table_created = True
                column_types_map = ydb.BulkUpsertColumns()
                for col_name, col_ydb_type in column_types:
                    type_obj, _ = ydb_type_to_str(col_ydb_type, args.store_type.upper())
                    column_types_map.add_column(col_name, type_obj)

            print(f"[{label}] Upsert {len(results)} rows")
            ydb_wrapper.bulk_upsert_batches(
                table_path, results, column_types_map, batch_size, query_name
            )

        print("By-month upload done.")


if __name__ == "__main__":
    sys.exit(main() or 0)
