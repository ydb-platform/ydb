#!/usr/bin/env python3
"""
Fetch and aggregate test data directly from test_results (test_runs_column).
No dependency on flaky_tests_window or tests_monitor.

Returns rows in the same format as tests_monitor for use with mute_logic.aggregate_test_data.
"""

import datetime
import logging
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'analytics'))
from ydb_wrapper import YDBWrapper

from regression_jobs import REGRESSION_JOB_NAMES, EXCLUDE_MANUAL_RUNS_SQL

# Filter out suite-level entries (same as create_new_muted_ya)
SUITE_TEST_NAMES = {'unittest', 'py3test', 'gtest'}


def _regression_jobs_sql():
    return ", ".join(f"'{j}'" for j in REGRESSION_JOB_NAMES)


def fetch_from_test_results(ydb_wrapper, branch, build_type, days_window=7, mute_check=None):
    """
    Fetch test runs from test_results, aggregate by (full_name, date_window).
    Returns list of dicts compatible with mute_logic.aggregate_test_data.

    mute_check: callable(suite_folder, test_name) -> bool, used to set is_muted per test.
    """
    table_path = ydb_wrapper.get_table_path("test_results")
    query = f"""
    SELECT
        full_name,
        suite_folder,
        test_name,
        CAST(run_timestamp AS Date) AS date_window,
        build_type,
        branch,
        SUM(CASE WHEN status = 'passed' THEN 1 ELSE 0 END) AS pass_count,
        SUM(CASE WHEN status IN ('failure', 'error') THEN 1 ELSE 0 END) AS fail_count,
        SUM(CASE WHEN status = 'mute' THEN 1 ELSE 0 END) AS mute_count,
        SUM(CASE WHEN status = 'skipped' THEN 1 ELSE 0 END) AS skip_count
    FROM `{table_path}`
    WHERE run_timestamp >= CurrentUtcDate() - {days_window} * Interval("P1D")
        AND branch = '{branch}'
        AND build_type = '{build_type}'
        AND job_name IN ({_regression_jobs_sql()})
        {EXCLUDE_MANUAL_RUNS_SQL}
        AND test_name IS NOT NULL
        AND test_name != ''
    GROUP BY full_name, suite_folder, test_name, date_window, build_type, branch
    """
    logging.info(f"Fetching from test_results for branch={branch}, build_type={build_type}, days={days_window}")
    results = ydb_wrapper.execute_scan_query(query, query_name=f"fetch_test_results_{branch}_{build_type}")

    # Filter suite-level tests
    filtered = [
        r for r in results
        if r.get('test_name') and r.get('test_name') not in SUITE_TEST_NAMES
    ]
    logging.info(f"Fetched {len(results)} rows, after filtering suite tests: {len(filtered)}")

    # Add is_muted, owner, state, is_test_chunk
    for row in filtered:
        suite = row.get('suite_folder') or ''
        name = row.get('test_name') or ''
        row['is_muted'] = 1 if (mute_check and mute_check(suite, name)) else 0
        row['owner'] = row.get('owner') or 'N/A'
        row['state'] = 'N/A'
        row['days_in_state'] = 0
        row['is_test_chunk'] = 1 if ('chunk' in (name or '').lower()) else 0

    return filtered
