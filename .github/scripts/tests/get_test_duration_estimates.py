#!/usr/bin/env python3
"""Fetch suite-level duration p90 from YDB test_results for shard planning."""
from __future__ import annotations

import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "analytics"))
from ydb_wrapper import YDBWrapper  # noqa: E402

MAX_DAYS_BACK = 180
DEFAULT_DAYS_BACK = 14
SUITE_IN_CLAUSE_THRESHOLD = 50


def _sql_string_list(values: list[str]) -> str:
    return ",".join("'{0}'".format(value.replace("'", "''")) for value in values)


def _decode_value(value: object) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


def expand_suite_path_prefixes(suite_paths: list[str]) -> list[str]:
    """Include parent folders so longest-prefix history match can resolve."""
    expanded: set[str] = set()
    for path in suite_paths:
        cleaned = path.strip().rstrip("/")
        if not cleaned:
            continue
        parts = cleaned.split("/")
        for index in range(1, len(parts) + 1):
            expanded.add("/".join(parts[:index]))
    return sorted(expanded)


def suite_related_to_requested(suite_folder: str, requested: set[str]) -> bool:
    """True if suite_folder equals a requested path or is a prefix/child of one."""
    if suite_folder in requested:
        return True
    prefix = suite_folder + "/"
    for path in requested:
        if path.startswith(prefix) or suite_folder.startswith(path + "/"):
            return True
    return False


def get_suite_duration_p90(
    suite_paths: list[str],
    days_back: int,
    build_type: str,
    branch: str,
    *,
    in_clause_threshold: int = SUITE_IN_CLAUSE_THRESHOLD,
) -> dict[str, float]:
    """Return {suite_folder: p90_total_duration_sec} for suites with history.

    Duration per suite is p90 over job runs of SUM(test durations) in that suite.
    If len(suite_paths) < in_clause_threshold, filter in SQL (including parent
    prefixes); otherwise fetch all matching rows for branch/build_type and keep
    suites related to the request by prefix so longest-prefix match works.
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

    requested = {path.strip().rstrip("/") for path in suite_paths if path.strip()}
    lookup_paths = expand_suite_path_prefixes(sorted(requested))
    use_in_clause = len(lookup_paths) < in_clause_threshold
    suite_filter = ""
    if use_in_clause:
        suite_filter = f"AND suite_folder IN [{_sql_string_list(lookup_paths)}]"

    with YDBWrapper(silent=True) as ydb_wrapper:
        if not ydb_wrapper.check_credentials():
            print("Warning: YDB credentials not found, skipping duration history", file=sys.stderr)
            return {}

        test_runs_table = ydb_wrapper.get_table_path("test_results")
        print(
            f"Querying suite duration p90 for {len(requested)} suites "
            f"({'IN clause' if use_in_clause else 'wide scan + prefix filter'}, "
            f"lookup_keys={len(lookup_paths)}): "
            f"build_type={build_type}, branch={branch}, days_back={days_back}",
            file=sys.stderr,
        )

        query = f"""
    PRAGMA AnsiInForEmptyOrNullableItemsCollections;
    SELECT
        suite_folder,
        PERCENTILE(run_suite_duration, 0.9) AS p90_sec
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
        rows = ydb_wrapper.execute_scan_query(query, query_name="shard_plan_suite_duration_p90")
        results: dict[str, float] = {}
        for row in rows:
            suite_folder = _decode_value(row["suite_folder"])
            if not use_in_clause and not suite_related_to_requested(suite_folder, requested):
                continue
            p90 = row["p90_sec"]
            if p90 is None:
                continue
            results[suite_folder] = float(p90)

        print(
            f"Retrieved suite duration p90 for {len(results)} suites "
            f"(requested {len(requested)} paths, days_back={days_back})",
            file=sys.stderr,
        )
        return results


# Backward-compatible alias (shard planning now uses p90).
get_suite_duration_p50 = get_suite_duration_p90
