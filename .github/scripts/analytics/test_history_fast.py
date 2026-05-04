#!/usr/bin/env python3

import os
import sys
import time

import ydb
from ydb_wrapper import YDBWrapper

TESTS_DIR = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'tests'))
if TESTS_DIR not in sys.path:
    sys.path.insert(0, TESTS_DIR)
from error_type_utils import (  # noqa: E402
    DEFAULT_PREFETCH_MAX_WORKERS,
    build_error_type_csv_for_storage,
    debug_file_texts_from_cache,
    failure_row_from_ydb,
    is_failure_like_status,
    normalize_fetch_url,
    prefetch_text_cache_and_urls_for_failure_rows,
    should_prefetch_debug_files,
    source_has_tag,
)

# stderr/log HTTP prefetch parallelism (same numbers as error_type_utils.DEFAULT_PREFETCH_MAX_WORKERS).
_DEFAULT_PREFETCH_WORKERS = DEFAULT_PREFETCH_MAX_WORKERS
_FULL_DAY_REFRESH_PREFETCH_WORKERS = 200


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


def get_missed_data_for_upload(
    ydb_wrapper,
    test_runs_table,
    test_history_fast_table,
    full_day_refresh=False,
    prefetch_max_workers=None,
):
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

    print(f"scan done: {len(results)} rows; building failure rows for stderr/log prefetch...", flush=True)
    failure_rows = [failure_row_from_ydb(row) for row in results if is_failure_like_status(row.get("status"))]
    fetch_cache, prefetch_debug_file_urls = prefetch_text_cache_and_urls_for_failure_rows(
        failure_rows,
        max_workers=prefetch_max_workers,
    )
    if prefetch_debug_file_urls:
        norm = {normalize_fetch_url(u) for u in prefetch_debug_file_urls}
        total_urls = len(norm)
        failed_count = sum(1 for u in norm if fetch_cache.get(u) is None)
        print(
            f"debug file prefetch: unique_urls={total_urls}, fetch_failed={failed_count}",
            flush=True,
        )
    else:
        print("debug file prefetch: no urls to download", flush=True)

    nrows = len(results)
    print(
        f"[classify] building comma-separated error_type for {nrows} rows (CPU, no network)...",
        flush=True,
    )
    verify_count = 0
    t_classify = time.time()
    if nrows > 5000:
        progress_step = max(5000, nrows // 20)
    elif nrows > 200:
        progress_step = max(200, nrows // 5)
    else:
        progress_step = 0
    for i, row in enumerate(results, 1):
        need = should_prefetch_debug_files(row.get("status"), row.get("stderr"), row.get("log"))
        stderr_text, log_text = debug_file_texts_from_cache(
            need, row.get("stderr"), row.get("log"), fetch_cache
        )
        row["error_type"] = build_error_type_csv_for_storage(
            row.get("status"),
            row.get("status_description"),
            row.get("error_type"),
            stderr_text,
            log_text,
            row.get("status"),
        )
        if source_has_tag(row["error_type"], "VERIFY"):
            verify_count += 1
        if i < nrows and progress_step > 0 and i % progress_step == 0:
            print(
                f"[classify] progress {i}/{nrows} rows ({time.time() - t_classify:.1f}s elapsed)",
                flush=True,
            )
    print(
        f"[classify] done {nrows} rows in {time.time() - t_classify:.1f}s, verify_tag={verify_count}",
        flush=True,
    )
    return results


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--full-day-refresh",
        action="store_true",
        help=(
            "Re-upload all data for the last day (disable dedup by existing test_id in fast table). "
            f"Uses {_FULL_DAY_REFRESH_PREFETCH_WORKERS} parallel workers for stderr/log prefetch "
            f"(default run uses {_DEFAULT_PREFETCH_WORKERS})."
        ),
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
        prefetch_max_workers = (
            _FULL_DAY_REFRESH_PREFETCH_WORKERS if args.full_day_refresh else _DEFAULT_PREFETCH_WORKERS
        )
        prepared_for_upload_rows = get_missed_data_for_upload(
            ydb_wrapper,
            test_runs_table,
            test_history_fast_table,
            full_day_refresh=args.full_day_refresh,
            prefetch_max_workers=prefetch_max_workers,
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
