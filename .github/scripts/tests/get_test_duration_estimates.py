#!/usr/bin/env python3
"""Fetch suite-level duration p50 from YDB test_results for shard planning."""
from __future__ import annotations

import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "analytics"))
from ydb_wrapper import YDBWrapper  # noqa: E402

MAX_DAYS_BACK = 180
DEFAULT_DAYS_BACK = 3
SUITE_IN_CLAUSE_THRESHOLD = 50


def _sql_string_list(values: list[str]) -> str:
    return ",".join("'{0}'".format(value.replace("'", "''")) for value in values)


def _decode_value(value: object) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


def get_suite_duration_p50(
    suite_paths: list[str],
    days_back: int,
    build_type: str,
    branch: str,
    *,
    in_clause_threshold: int = SUITE_IN_CLAUSE_THRESHOLD,
) -> dict[str, float]:
    """Return {suite_folder: p50_total_duration_sec} for suites with history.

    Duration per suite is p50 over job runs of SUM(test durations) in that suite.
    If len(suite_paths) < in_clause_threshold, filter in SQL; otherwise fetch all
    matching rows for branch/build_type and filter client-side.
    """
    if not suite_paths:
        return {}

    if days_back > MAX_DAYS_BACK:
        print(
            f"Warning: days_back ({days_back}) exceeds maximum ({MAX_DAYS_BACK}), "
            f"capping to {MAX_DAYS_BACK}",
            file=sys.stderr,
        )
        days_back = MAX_DAYS_BACK

    requested = set(suite_paths)
    use_in_clause = len(suite_paths) < in_clause_threshold
    suite_filter = ""
    if use_in_clause:
        suite_filter = f"AND suite_folder IN [{_sql_string_list(suite_paths)}]"

    with YDBWrapper(silent=True) as ydb_wrapper:
        if not ydb_wrapper.check_credentials():
            print("Warning: YDB credentials not found, skipping duration history", file=sys.stderr)
            return {}

        test_runs_table = ydb_wrapper.get_table_path("test_results")
        print(
            f"Querying suite duration p50 for {len(suite_paths)} suites "
            f"({'IN clause' if use_in_clause else 'wide scan + client filter'}): "
            f"build_type={build_type}, branch={branch}, days_back={days_back}",
            file=sys.stderr,
        )

        query = f"""
    PRAGMA AnsiInForEmptyOrNullableItemsCollections;
    SELECT
        suite_folder,
        PERCENTILE(run_suite_duration, 0.5) AS p50_sec
    FROM (
        SELECT
            suite_folder,
            job_id,
            run_timestamp,
            SUM(duration) AS run_suite_duration
        FROM
            `{test_runs_table}` AS t
        WHERE
            t.status != 'skipped'
            AND t.run_timestamp > CurrentUtcDate() - {days_back} * Interval("P1D")
            AND ('{build_type}' = '' OR t.build_type = '{build_type}')
            AND ('{branch}' = '' OR t.branch = '{branch}')
            AND t.job_name != 'Run-tests'
            AND (t.pull IS NULL OR NOT String::Contains(t.pull, 'manual'))
            AND t.duration IS NOT NULL
            AND t.duration > 0
            {suite_filter}
        GROUP BY
            suite_folder,
            job_id,
            run_timestamp
    ) AS runs
    GROUP BY
        suite_folder;
"""
        rows = ydb_wrapper.execute_scan_query(query, query_name="shard_plan_suite_duration_p50")
        results: dict[str, float] = {}
        for row in rows:
            suite_folder = _decode_value(row["suite_folder"])
            if not use_in_clause and suite_folder not in requested:
                continue
            p50 = row["p50_sec"]
            if p50 is None:
                continue
            results[suite_folder] = float(p50)

        print(
            f"Retrieved suite duration p50 for {len(results)} suites "
            f"out of {len(suite_paths)} requested",
            file=sys.stderr,
        )
        return results
