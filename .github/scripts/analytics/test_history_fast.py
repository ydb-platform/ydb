#!/usr/bin/env python3

import ydb
import os
import sys
from ydb_wrapper import YDBWrapper

TESTS_DIR = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'tests'))
if TESTS_DIR not in sys.path:
    sys.path.insert(0, TESTS_DIR)
from error_type_utils import classify_error_type, prefetch_texts_by_urls  # noqa: E402


def create_test_history_fast_table(ydb_wrapper, table_path):
    print(f"> Creating table: '{table_path}'")
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS  `{table_path}` (
            `build_type` Utf8 NOT NULL,
            `job_name` Utf8 NOT NULL,
            `job_id` Uint64,
            `commit` Utf8,
            `branch` Utf8 NOT NULL,
            `pull` Utf8,
            `run_timestamp` Timestamp NOT NULL,
            `test_id` Utf8 NOT NULL,
            `suite_folder` Utf8,
            `test_name` Utf8,
            `full_name` Utf8 NOT NULL,
            `duration` Double,
            `status` Utf8,
            `status_description` Utf8,
            `error_type` Utf8,
            `owners` Utf8,
            `log` Utf8,
            `logsdir` Utf8,
            `stderr` Utf8,
            `stdout` Utf8,
            PRIMARY KEY (`run_timestamp`, `build_type`, `branch`, `full_name`, `job_name`, `test_id`)
        )
        PARTITION BY HASH(`run_timestamp`, `build_type`, `branch`, `full_name`)
        WITH (
        STORE = COLUMN,
        TTL = Interval("P60D") ON run_timestamp
        )
    """
    ydb_wrapper.create_table(table_path, create_sql)


def get_missed_data_for_upload(ydb_wrapper, test_runs_table, test_history_fast_table, full_day_refresh=False):
    join_clause = ""
    dedup_where_clause = ""
    if not full_day_refresh:
        join_clause = f"""
    LEFT JOIN (
        select distinct test_id  from `{test_history_fast_table}`
        where run_timestamp >= CurrentUtcDate() - 1*Interval("P1D")
    ) as fast_data_missed
    ON all_data.test_id = fast_data_missed.test_id
"""
        dedup_where_clause = "and fast_data_missed.test_id is NULL"

    query = f"""
       SELECT 
        build_type, 
        job_name, 
        job_id, 
        commit, 
        branch, 
        pull, 
        run_timestamp, 
        all_data.test_id as test_id, 
        suite_folder, 
        test_name,
        cast(suite_folder || '/' || test_name as UTF8)  as full_name, 
        duration,
        status,
        status_description,
        error_type,
        owners,
        log,
        logsdir,
        stderr,
        stdout
    FROM `{test_runs_table}`  as all_data
    {join_clause}
    WHERE
        all_data.run_timestamp >= CurrentUtcDate() - 1*Interval("P1D")
        and String::Contains(all_data.test_name, '.flake8')  = FALSE
        and (CASE 
            WHEN String::Contains(all_data.test_name, 'sole chunk') 
                OR String::Contains(all_data.test_name, 'chunk+chunk') 
                OR String::Contains(all_data.test_name, '[chunk]') 
            THEN TRUE
            ELSE FALSE
            END) = FALSE
        and (all_data.branch = 'main' or all_data.branch like 'stable-%' or all_data.branch like 'stream-nb-2%')
        {dedup_where_clause}
    """

    print(f'missed data capturing')
    results = ydb_wrapper.execute_scan_query(query, query_name="get_missed_data_for_upload")
    stderr_urls = [row.get("stderr") for row in results]
    stderr_fetch_cache = prefetch_texts_by_urls(stderr_urls)
    if stderr_urls:
        total_urls = len({url for url in stderr_urls if url})
        failed_count = sum(1 for url in {url for url in stderr_urls if url} if not stderr_fetch_cache.get(url))
        print(f"stderr prefetch: done, total={total_urls}, success={total_urls - failed_count}, failed={failed_count}")
    else:
        print("stderr prefetch: no urls to download")
    verify_count = 0
    for row in results:
        stderr_url = row.get("stderr")
        stderr_text = stderr_fetch_cache.get(stderr_url, "")
        error_type = classify_error_type(
            row.get("status"),
            row.get("status_description"),
            row.get("error_type"),
            verify_source_text=stderr_text,
        )

        row["error_type"] = error_type
        if error_type == "VERIFY":
            verify_count += 1
    print(f"classification summary: rows={len(results)}, verify={verify_count}")
    return results


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--full-day-refresh",
        action="store_true",
        help="Re-upload all data for the last day (disable dedup by existing test_id in fast table)",
    )
    args = parser.parse_args()

    with YDBWrapper() as ydb_wrapper:
        
        
        # Check credentials
        if not ydb_wrapper.check_credentials():
            return 1
        
        # Get table paths from config
        test_runs_table = ydb_wrapper.get_table_path("test_results")
        test_history_fast_table = ydb_wrapper.get_table_path("test_history_fast")
        
        table_path = test_history_fast_table
        batch_size = 1000

        # Create table if it doesn't exist (wrapper will add database_path automatically)
        create_test_history_fast_table(ydb_wrapper, table_path)
        
        # Get missed data for upload
        prepared_for_upload_rows = get_missed_data_for_upload(
            ydb_wrapper,
            test_runs_table,
            test_history_fast_table,
            full_day_refresh=args.full_day_refresh,
        )
        print(f'Preparing to upsert: {len(prepared_for_upload_rows)} rows')
        
        if prepared_for_upload_rows:
            # Prepare column_types once (same fields as returned by get_missed_data_for_upload)
            column_types = (
                ydb.BulkUpsertColumns()
                .add_column("build_type", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                .add_column("job_name", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                .add_column("job_id", ydb.OptionalType(ydb.PrimitiveType.Uint64))
                .add_column("commit", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                .add_column("branch", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                .add_column("pull", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                .add_column("run_timestamp", ydb.OptionalType(ydb.PrimitiveType.Timestamp))
                .add_column("test_id", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                .add_column("suite_folder", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                .add_column("test_name", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                .add_column("full_name", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                .add_column("duration", ydb.OptionalType(ydb.PrimitiveType.Double))
                .add_column("status", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                .add_column("status_description", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                .add_column("error_type", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                .add_column("owners", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                .add_column("log", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                .add_column("logsdir", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                .add_column("stderr", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                .add_column("stdout", ydb.OptionalType(ydb.PrimitiveType.Utf8))
            )
            
            # Use bulk_upsert_batches for aggregated statistics (wrapper will add database_path automatically)
            ydb_wrapper.bulk_upsert_batches(table_path, prepared_for_upload_rows, column_types, batch_size)
            print('Tests uploaded')
        else:
            print('Nothing to upload')


if __name__ == "__main__":
    main()
