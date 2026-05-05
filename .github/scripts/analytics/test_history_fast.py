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
    DEFAULT_PREFETCH_MAX_WORKERS_FULL_REFRESH,
    build_error_type_csv_for_storage,
    failure_row_from_ydb,
    get_debug_texts_from_cache,
    is_failure_like_status,
    prefetch_text_cache_for_failure_rows,
    source_has_tag,
)


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

    print('missed data capturing')
    results = ydb_wrapper.execute_scan_query(query, query_name="get_missed_data_for_upload")

    print(f"scan done: {len(results)} rows; building failure rows for prefetch...", flush=True)
    row_pairs = [
        (row, failure_row_from_ydb(row))
        for row in results
        if is_failure_like_status(row.get("status"))
    ]

    fetch_cache = prefetch_text_cache_for_failure_rows(
        [fr for _, fr in row_pairs],
        max_workers=prefetch_max_workers,
    )

    # Classify failure rows and write error_type back into the row dict in-place
    # (the same dict is later passed to bulk_upsert_batches).
    total = len(row_pairs)
    print(f"[classify] {total} failure rows...", flush=True)
    verify_count = 0
    sanitizer_count = 0
    t_classify = time.time()
    progress_step = max(200, total // 10) if total > 200 else 0

    for i, (row, fr) in enumerate(row_pairs, 1):
        stderr_text, log_text = get_debug_texts_from_cache(fr, fetch_cache)
        row["error_type"] = build_error_type_csv_for_storage(
            fr.status,
            fr.status_description,
            fr.source_error_type,
            stderr_text,
            log_text,
            status_name_for_not_launched=fr.status,
        )
        if source_has_tag(row["error_type"], "VERIFY"):
            verify_count += 1
        if source_has_tag(row["error_type"], "SANITIZER"):
            sanitizer_count += 1
        if progress_step and i % progress_step == 0 and i < total:
            print(f"[classify] {i}/{total} ({time.time() - t_classify:.1f}s)", flush=True)
    print(
        f"[classify] done {total} rows in {time.time() - t_classify:.1f}s, "
        f"verify={verify_count}, sanitizer={sanitizer_count}",
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
            f"Uses {DEFAULT_PREFETCH_MAX_WORKERS_FULL_REFRESH} parallel workers for stderr/log prefetch "
            f"(default run uses {DEFAULT_PREFETCH_MAX_WORKERS})."
        ),
    )
    args = parser.parse_args()

    with YDBWrapper() as ydb_wrapper:
        if not ydb_wrapper.check_credentials():
            return 1

        test_runs_table = ydb_wrapper.get_table_path("test_results")
        test_history_fast_table = ydb_wrapper.get_table_path("test_history_fast")
        batch_size = 1000

        create_test_history_fast_table(ydb_wrapper, test_history_fast_table)
        
        prefetch_max_workers = (
            DEFAULT_PREFETCH_MAX_WORKERS_FULL_REFRESH if args.full_day_refresh else DEFAULT_PREFETCH_MAX_WORKERS
        )
        rows = get_missed_data_for_upload(
            ydb_wrapper,
            test_runs_table,
            test_history_fast_table,
            full_day_refresh=args.full_day_refresh,
            prefetch_max_workers=prefetch_max_workers,
        )
        print(f'Preparing to upsert: {len(rows)} rows')

        if rows:
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
            
            ydb_wrapper.bulk_upsert_batches(test_history_fast_table, rows, column_types, batch_size)
            print('Tests uploaded')
        else:
            print('Nothing to upload')


if __name__ == "__main__":
    main()
