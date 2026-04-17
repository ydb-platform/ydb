#!/usr/bin/env python3

import argparse
import datetime
import json
import os
import hashlib
import sys
from typing import Dict, FrozenSet, List, Optional, Tuple

import ydb

from ydb_wrapper import YDBWrapper

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from github_issue_utils import DEFAULT_BUILD_TYPE, parse_body
from mute_policy_rules import (
    is_delete_candidate_counts,
    load_mute_coordinator_thresholds as _load_thresholds,
    passes_default_mute,
    passes_default_unmute,
)


VALID_POLICY_TYPES = {
    "default_unmute",
    "quarantine_new_test",
    "quarantine_user_fixed",
    "quarantine_manual_unmute",
    "quarantine_manual_mute",
}

# control_state.lifecycle_state — only three coarse values; use policy_type (+ decision_reason) for detail.
LS_MUTED = "muted"
LS_UNMUTED = "unmuted"
LS_QUARANTINE = "quarantine"

_LIFECYCLE_COARSE: FrozenSet[str] = frozenset({LS_MUTED, LS_UNMUTED, LS_QUARANTINE})


def _canon_lifecycle_state(lifecycle: Optional[str], policy_type: Optional[str]) -> str:
    """Normalize lifecycle_state from DB (supports legacy quarantine_by_* / *_by_rules)."""
    life = str(lifecycle or "").strip()
    pol = str(policy_type or "").strip()
    if life in _LIFECYCLE_COARSE:
        return life
    if life.startswith("quarantine_by_"):
        return LS_QUARANTINE
    if life == "muted_by_rules":
        return LS_MUTED
    if life == "unmuted_by_rules":
        return LS_UNMUTED
    if pol.startswith("quarantine_"):
        return LS_QUARANTINE
    if pol == "default_unmute" or pol == "":
        return LS_MUTED
    return LS_MUTED


def _parse_date(value: str) -> datetime.date:
    return datetime.datetime.strptime(value, "%Y-%m-%d").date()


def _date_expr(as_of_date: Optional[datetime.date]) -> str:
    if as_of_date is None:
        return "CurrentUtcDate()"
    return f"Date('{as_of_date.isoformat()}')"


def _to_utc_datetime(value: object) -> Optional[datetime.datetime]:
    if value is None:
        return None
    if isinstance(value, datetime.datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=datetime.timezone.utc)
        return value.astimezone(datetime.timezone.utc)
    if isinstance(value, (int, float)):
        # YDB Timestamp is usually microseconds since epoch.
        ts = float(value)
        if abs(ts) > 1e12:
            ts = ts / 1_000_000.0
        return datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc)
    if isinstance(value, str):
        normalized = value.strip().replace("Z", "+00:00")
        try:
            dt = datetime.datetime.fromisoformat(normalized)
        except ValueError:
            return None
        if dt.tzinfo is None:
            return dt.replace(tzinfo=datetime.timezone.utc)
        return dt.astimezone(datetime.timezone.utc)
    return None


def _create_control_state_table(ydb_wrapper: YDBWrapper, table_path: str) -> None:
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS `{table_path}` (
            `branch` Utf8 NOT NULL,
            `build_type` Utf8 NOT NULL,
            `full_name` Utf8 NOT NULL,
            `issue_number` Uint64,
            `lifecycle_state` Utf8 NOT NULL, -- muted | unmuted | quarantine
            `policy_type` Utf8 NOT NULL,
            `policy_version` Utf8 NOT NULL,
            `policy_params_json` Json,
            `request_source` Utf8,
            `requested_by_login` Utf8,
            `requested_by_type` Utf8,
            `decision_reason` Utf8,
            `state_entered_at` Timestamp NOT NULL,
            `state_until` Timestamp,
            `last_evaluated_at` Timestamp,
            `active` Uint8 NOT NULL,
            `updated_at` Timestamp NOT NULL,
            PRIMARY KEY (`branch`, `build_type`, `full_name`)
        )
        PARTITION BY HASH(`branch`, `build_type`)
        WITH (STORE = COLUMN)
    """
    ydb_wrapper.create_table(table_path, create_sql)


def _create_control_events_table(ydb_wrapper: YDBWrapper, table_path: str) -> None:
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS `{table_path}` (
            `event_date` Date NOT NULL,
            `event_id` Utf8 NOT NULL,
            `event_time` Timestamp NOT NULL,
            `branch` Utf8 NOT NULL,
            `build_type` Utf8 NOT NULL,
            `full_name` Utf8 NOT NULL,
            `issue_number` Uint64,
            `event_type` Utf8 NOT NULL,
            `before_state` Utf8,
            `after_state` Utf8,
            `before_policy` Utf8,
            `after_policy` Utf8,
            `actor_login` Utf8,
            `actor_type` Utf8,
            `payload_json` Json,
            PRIMARY KEY (`event_date`, `event_id`)
        )
        PARTITION BY HASH(`event_date`)
        WITH (STORE = COLUMN)
    """
    ydb_wrapper.create_table(table_path, create_sql)


def _create_effective_rule_table(ydb_wrapper: YDBWrapper, table_path: str) -> None:
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS `{table_path}` (
            `branch` Utf8 NOT NULL,
            `build_type` Utf8 NOT NULL,
            `full_name` Utf8 NOT NULL,
            `issue_number` Uint64,
            `effective_rule_type` Utf8 NOT NULL,
            `effective_window_days` Uint32 NOT NULL,
            `effective_min_runs` Uint32 NOT NULL,
            `effective_fail_limit` Uint32 NOT NULL,
            `rule_source_state` Utf8 NOT NULL,
            `rule_valid_from` Timestamp NOT NULL,
            `rule_valid_until` Timestamp,
            `computed_at` Timestamp NOT NULL,
            PRIMARY KEY (`branch`, `build_type`, `full_name`)
        )
        PARTITION BY HASH(`branch`, `build_type`)
        WITH (STORE = COLUMN)
    """
    ydb_wrapper.create_table(table_path, create_sql)


def _create_tables(ydb_wrapper: YDBWrapper) -> Dict[str, str]:
    paths = {
        "control_state": ydb_wrapper.get_table_path("mute_coordinator_control_state"),
        "control_events": ydb_wrapper.get_table_path("mute_coordinator_control_events"),
        "effective_rule": ydb_wrapper.get_table_path("mute_coordinator_effective_rule"),
    }
    _create_control_state_table(ydb_wrapper, paths["control_state"])
    _create_control_events_table(ydb_wrapper, paths["control_events"])
    _create_effective_rule_table(ydb_wrapper, paths["effective_rule"])
    return paths


def _fetch_current_muted_tests(
    ydb_wrapper: YDBWrapper,
    branch: str,
    build_type: str,
    as_of_date: Optional[datetime.date] = None,
) -> List[str]:
    tests_monitor_path = ydb_wrapper.get_table_path("tests_monitor")
    anchor_date = _date_expr(as_of_date)
    query = f"""
        $latest = (
            SELECT MAX(date_window) AS latest_date
            FROM `{tests_monitor_path}`
            WHERE branch = '{branch}'
              AND build_type = '{build_type}'
              AND date_window <= {anchor_date}
        );

        SELECT DISTINCT tm.full_name AS full_name
        FROM `{tests_monitor_path}` AS tm
        INNER JOIN $latest AS l ON tm.date_window = l.latest_date
        WHERE tm.branch = '{branch}'
          AND tm.build_type = '{build_type}'
          AND tm.is_muted = 1
    """
    rows = ydb_wrapper.execute_scan_query(query, query_name=f"mute_coordinator_current_muted_{branch}_{build_type}")
    return sorted(str(r["full_name"]) for r in rows if r.get("full_name"))


def _fetch_existing_control_state(
    ydb_wrapper: YDBWrapper,
    control_state_path: str,
    branch: str,
    build_type: str,
) -> Dict[str, Dict[str, object]]:
    query = f"""
        SELECT
            full_name,
            issue_number,
            lifecycle_state,
            policy_type,
            policy_version,
            policy_params_json,
            request_source,
            requested_by_login,
            requested_by_type,
            decision_reason,
            state_entered_at,
            state_until,
            last_evaluated_at,
            active,
            updated_at
        FROM `{control_state_path}`
        WHERE branch = '{branch}'
          AND build_type = '{build_type}'
          AND active = 1
    """
    rows = ydb_wrapper.execute_scan_query(
        query,
        query_name=f"mute_coordinator_existing_state_{branch}_{build_type}",
    )
    out: Dict[str, Dict[str, object]] = {}
    for row in rows:
        full_name = row.get("full_name")
        if not full_name:
            continue
        row["state_entered_at"] = _to_utc_datetime(row.get("state_entered_at"))
        row["state_until"] = _to_utc_datetime(row.get("state_until"))
        row["last_evaluated_at"] = _to_utc_datetime(row.get("last_evaluated_at"))
        row["updated_at"] = _to_utc_datetime(row.get("updated_at"))
        row["lifecycle_state"] = _canon_lifecycle_state(
            str(row.get("lifecycle_state") or ""),
            str(row.get("policy_type") or ""),
        )
        out[str(full_name)] = row
    return out


def _build_column_types_for_effective_rule() -> ydb.BulkUpsertColumns:
    return (
        ydb.BulkUpsertColumns()
        .add_column("branch", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("build_type", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("full_name", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("issue_number", ydb.OptionalType(ydb.PrimitiveType.Uint64))
        .add_column("effective_rule_type", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("effective_window_days", ydb.OptionalType(ydb.PrimitiveType.Uint32))
        .add_column("effective_min_runs", ydb.OptionalType(ydb.PrimitiveType.Uint32))
        .add_column("effective_fail_limit", ydb.OptionalType(ydb.PrimitiveType.Uint32))
        .add_column("rule_source_state", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("rule_valid_from", ydb.OptionalType(ydb.PrimitiveType.Timestamp))
        .add_column("rule_valid_until", ydb.OptionalType(ydb.PrimitiveType.Timestamp))
        .add_column("computed_at", ydb.OptionalType(ydb.PrimitiveType.Timestamp))
    )


def _build_column_types_for_control_state() -> ydb.BulkUpsertColumns:
    return (
        ydb.BulkUpsertColumns()
        .add_column("branch", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("build_type", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("full_name", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("issue_number", ydb.OptionalType(ydb.PrimitiveType.Uint64))
        .add_column("lifecycle_state", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("policy_type", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("policy_version", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("policy_params_json", ydb.OptionalType(ydb.PrimitiveType.Json))
        .add_column("request_source", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("requested_by_login", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("requested_by_type", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("decision_reason", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("state_entered_at", ydb.OptionalType(ydb.PrimitiveType.Timestamp))
        .add_column("state_until", ydb.OptionalType(ydb.PrimitiveType.Timestamp))
        .add_column("last_evaluated_at", ydb.OptionalType(ydb.PrimitiveType.Timestamp))
        .add_column("active", ydb.OptionalType(ydb.PrimitiveType.Uint8))
        .add_column("updated_at", ydb.OptionalType(ydb.PrimitiveType.Timestamp))
    )


def _build_column_types_for_control_events() -> ydb.BulkUpsertColumns:
    return (
        ydb.BulkUpsertColumns()
        .add_column("event_date", ydb.OptionalType(ydb.PrimitiveType.Date))
        .add_column("event_id", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("event_time", ydb.OptionalType(ydb.PrimitiveType.Timestamp))
        .add_column("branch", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("build_type", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("full_name", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("issue_number", ydb.OptionalType(ydb.PrimitiveType.Uint64))
        .add_column("event_type", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("before_state", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("after_state", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("before_policy", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("after_policy", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("actor_login", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("actor_type", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("payload_json", ydb.OptionalType(ydb.PrimitiveType.Json))
    )


def _rule_window_days(rule_type: str, thresholds: Dict[str, int]) -> int:
    if rule_type == "quarantine_new_test":
        return int(thresholds["quarantine_new_test_window_days"])
    if rule_type == "quarantine_user_fixed":
        return int(thresholds["quarantine_user_fixed_window_days"])
    if rule_type == "quarantine_manual_unmute":
        return int(thresholds["quarantine_manual_unmute_window_days"])
    if rule_type == "quarantine_manual_mute":
        return int(thresholds["quarantine_manual_mute_window_days"])
    return int(thresholds["default_unmute_window_days"])


def _extract_issue_tests(issue_body: str, branch: str, build_type: str) -> List[str]:
    body = issue_body or ""
    if "<!--mute_list_start-->" not in body or "<!--mute_list_end-->" not in body:
        return []
    parsed = parse_body(body)
    issue_branches = parsed.branches or []
    issue_build_type = parsed.build_type or DEFAULT_BUILD_TYPE
    if issue_branches and branch not in issue_branches:
        return []
    if issue_build_type != build_type:
        return []
    return sorted(set(parsed.tests))


def _fetch_user_fixed_candidates(
    ydb_wrapper: YDBWrapper,
    branch: str,
    build_type: str,
    muted_tests: List[str],
    as_of_date: Optional[datetime.date] = None,
) -> Dict[str, int]:
    if not muted_tests:
        return {}
    issues_path = ydb_wrapper.get_table_path("issues")
    anchor_date = _date_expr(as_of_date)
    query_with_actor = f"""
        SELECT issue_number, body
        FROM `{issues_path}`
        WHERE state = 'CLOSED'
          AND closed_by_type = 'User'
          AND closed_at IS NOT NULL
          AND Cast(closed_at AS Date) >= {anchor_date} - 2*Interval("P1D")
          AND Cast(closed_at AS Date) <= {anchor_date}
    """
    query_legacy = f"""
        SELECT issue_number, body
        FROM `{issues_path}`
        WHERE state = 'CLOSED'
          AND closed_at IS NOT NULL
          AND Cast(closed_at AS Date) >= {anchor_date} - 2*Interval("P1D")
          AND Cast(closed_at AS Date) <= {anchor_date}
    """
    try:
        rows = ydb_wrapper.execute_scan_query(
            query_with_actor,
            query_name=f"mute_coordinator_user_fixed_candidates_{branch}_{build_type}",
        )
    except Exception as exc:
        msg = str(exc).lower()
        missing_actor_column = "member not found: closed_by_type" in msg
        if not missing_actor_column:
            raise
        print(
            "Warning: issues.closed_by_type is unavailable, "
            "falling back to legacy closed-issue filter "
            f"(branch={branch}, build_type={build_type})"
        )
        rows = ydb_wrapper.execute_scan_query(
            query_legacy,
            query_name=f"mute_coordinator_user_fixed_candidates_legacy_{branch}_{build_type}",
        )
    muted_set = set(muted_tests)
    result: Dict[str, int] = {}
    for row in rows:
        issue_number = int(row.get("issue_number") or 0)
        if issue_number <= 0:
            continue
        issue_tests = _extract_issue_tests(str(row.get("body") or ""), branch=branch, build_type=build_type)
        if not issue_tests:
            continue
        for full_name in sorted(set(issue_tests) & muted_set):
            if full_name not in result or issue_number > result[full_name]:
                result[full_name] = issue_number
    return result


def _fetch_new_test_candidates(
    ydb_wrapper: YDBWrapper,
    branch: str,
    build_type: str,
    as_of_date: Optional[datetime.date] = None,
) -> List[str]:
    tests_monitor_path = ydb_wrapper.get_table_path("tests_monitor")
    anchor_date = _date_expr(as_of_date)
    query = f"""
        $latest = (
            SELECT MAX(date_window) AS latest_date
            FROM `{tests_monitor_path}`
            WHERE branch = '{branch}'
              AND build_type = '{build_type}'
              AND date_window <= {anchor_date}
        );

        $first_seen = (
            SELECT
                full_name,
                MIN(date_window) AS first_seen_date
            FROM `{tests_monitor_path}`
            WHERE branch = '{branch}'
              AND build_type = '{build_type}'
              AND date_window <= {anchor_date}
            GROUP BY full_name
        );

        SELECT DISTINCT tm.full_name AS full_name
        FROM `{tests_monitor_path}` AS tm
        INNER JOIN $latest AS l ON tm.date_window = l.latest_date
        INNER JOIN $first_seen AS fs ON tm.full_name = fs.full_name
        WHERE tm.branch = '{branch}'
          AND tm.build_type = '{build_type}'
          AND tm.is_muted = 0
          AND tm.previous_state = 'no_runs'
          AND tm.date_window = fs.first_seen_date
          AND (COALESCE(tm.pass_count, 0) + COALESCE(tm.fail_count, 0) + COALESCE(tm.mute_count, 0)) > 0
    """
    rows = ydb_wrapper.execute_scan_query(
        query,
        query_name=f"mute_coordinator_new_test_candidates_{branch}_{build_type}",
    )
    return sorted(str(row["full_name"]) for row in rows if row.get("full_name"))


def _fetch_manual_unmute_candidates(
    ydb_wrapper: YDBWrapper,
    branch: str,
    build_type: str,
    thresholds: Dict[str, int],
    as_of_date: Optional[datetime.date] = None,
) -> List[str]:
    """Detect is_muted 1→0 where the window ending on transition day does not match default_unmute.

    Same window length as create_new_muted_ya (default_unmute_window_days), anchored on the monitor transition
    day (first day is_muted=0), not on the coordinator sync date.

    Skips quarantine when the case matches create_new_muted_ya delete-from-mutes criteria (no runs in window,
    or last muted day was only skips while muted).
    """
    tests_monitor_path = ydb_wrapper.get_table_path("tests_monitor")
    anchor_date = _date_expr(as_of_date)
    uw = max(int(thresholds["default_unmute_window_days"]), 1)
    span = max(uw - 1, 0)
    query = f"""
        $ranked = (
            SELECT
                full_name,
                date_window,
                is_muted,
                COALESCE(pass_count, 0) AS pass_count,
                COALESCE(fail_count, 0) AS fail_count,
                COALESCE(mute_count, 0) AS mute_count,
                COALESCE(skip_count, 0) AS skip_count,
                ROW_NUMBER() OVER (
                    PARTITION BY full_name
                    ORDER BY date_window DESC
                ) AS rn
            FROM `{tests_monitor_path}`
            WHERE branch = '{branch}'
              AND build_type = '{build_type}'
              AND date_window >= {anchor_date} - 14*Interval("P1D")
              AND date_window <= {anchor_date}
        );

        $transitions = (
            SELECT
                full_name,
                MAX(CASE WHEN rn = 1 THEN date_window ELSE NULL END) AS transition_date,
                MAX(CASE WHEN rn = 2 THEN pass_count ELSE 0 END) AS pre_muted_pass,
                MAX(CASE WHEN rn = 2 THEN fail_count ELSE 0 END) AS pre_muted_fail,
                MAX(CASE WHEN rn = 2 THEN mute_count ELSE 0 END) AS pre_muted_mute,
                MAX(CASE WHEN rn = 2 THEN skip_count ELSE 0 END) AS pre_muted_skip
            FROM $ranked
            GROUP BY full_name
            HAVING
                MAX(CASE WHEN rn = 1 THEN is_muted ELSE 0 END) = 0
                AND MAX(CASE WHEN rn = 2 THEN is_muted ELSE 0 END) = 1
        );

        SELECT
            m.full_name AS full_name,
            SUM(COALESCE(m.pass_count, 0)) AS pass_count,
            SUM(COALESCE(m.fail_count, 0)) AS fail_count,
            SUM(COALESCE(m.mute_count, 0)) AS mute_count,
            SUM(COALESCE(m.skip_count, 0)) AS skip_count,
            t.pre_muted_pass,
            t.pre_muted_fail,
            t.pre_muted_mute,
            t.pre_muted_skip
        FROM `{tests_monitor_path}` AS m
        INNER JOIN $transitions AS t ON m.full_name = t.full_name
        WHERE m.branch = '{branch}'
          AND m.build_type = '{build_type}'
          AND m.date_window >= t.transition_date - {span}*Interval("P1D")
          AND m.date_window <= t.transition_date
        GROUP BY
            m.full_name,
            t.pre_muted_pass,
            t.pre_muted_fail,
            t.pre_muted_mute,
            t.pre_muted_skip
    """
    stats_rows = ydb_wrapper.execute_scan_query(
        query,
        query_name=f"mute_coordinator_manual_unmute_stats_{branch}_{build_type}",
    )

    candidates: List[str] = []
    stats_by_name = {str(row["full_name"]): row for row in stats_rows if row.get("full_name")}
    for full_name in sorted(stats_by_name.keys()):
        row = stats_by_name.get(full_name, {})
        pass_count = int(row.get("pass_count") or 0)
        fail_count = int(row.get("fail_count") or 0)
        mute_count = int(row.get("mute_count") or 0)
        skip_count = int(row.get("skip_count") or 0)
        pre_p = int(row.get("pre_muted_pass") or 0)
        pre_f = int(row.get("pre_muted_fail") or 0)
        pre_m = int(row.get("pre_muted_mute") or 0)
        pre_s = int(row.get("pre_muted_skip") or 0)
        if is_delete_candidate_counts(pre_p, pre_f, pre_m, pre_s, is_muted=True):
            continue
        if is_delete_candidate_counts(pass_count, fail_count, mute_count, skip_count, is_muted=False):
            continue
        if not passes_default_unmute(pass_count, fail_count, mute_count, thresholds):
            candidates.append(full_name)
    return sorted(candidates)


def _fetch_manual_mute_candidates(
    ydb_wrapper: YDBWrapper,
    branch: str,
    build_type: str,
    thresholds: Dict[str, int],
    as_of_date: Optional[datetime.date] = None,
) -> List[str]:
    """Detect is_muted 0→1 where the pre-mute window does not match create_new_muted_ya mute rules.

    Must aggregate pass/fail over default_mute_window_days, ending on the mute
    transition day (not the coordinator sync anchor), otherwise ydbot mutes look like manual
    after a few green days.
    """
    tests_monitor_path = ydb_wrapper.get_table_path("tests_monitor")
    anchor_date = _date_expr(as_of_date)
    mw = max(int(thresholds["default_mute_window_days"]), 1)
    span = max(mw - 1, 0)
    query = f"""
        $ranked = (
            SELECT
                full_name,
                date_window,
                is_muted,
                ROW_NUMBER() OVER (
                    PARTITION BY full_name
                    ORDER BY date_window DESC
                ) AS rn
            FROM `{tests_monitor_path}`
            WHERE branch = '{branch}'
              AND build_type = '{build_type}'
              AND date_window >= {anchor_date} - 14*Interval("P1D")
              AND date_window <= {anchor_date}
        );

        $transitions = (
            SELECT
                full_name,
                MAX(CASE WHEN rn = 1 THEN date_window ELSE NULL END) AS transition_date
            FROM $ranked
            GROUP BY full_name
            HAVING
                MAX(CASE WHEN rn = 1 THEN is_muted ELSE 0 END) = 1
                AND MAX(CASE WHEN rn = 2 THEN is_muted ELSE 0 END) = 0
        );

        SELECT
            m.full_name AS full_name,
            SUM(COALESCE(m.pass_count, 0)) AS pass_count,
            SUM(COALESCE(m.fail_count, 0)) AS fail_count
        FROM `{tests_monitor_path}` AS m
        INNER JOIN $transitions AS t ON m.full_name = t.full_name
        WHERE m.branch = '{branch}'
          AND m.build_type = '{build_type}'
          AND m.date_window >= t.transition_date - {span}*Interval("P1D")
          AND m.date_window <= t.transition_date
        GROUP BY m.full_name
    """
    stats_rows = ydb_wrapper.execute_scan_query(
        query,
        query_name=f"mute_coordinator_manual_mute_stats_{branch}_{build_type}",
    )

    candidates: List[str] = []
    stats_by_name = {str(row["full_name"]): row for row in stats_rows if row.get("full_name")}
    for full_name in sorted(stats_by_name.keys()):
        row = stats_by_name.get(full_name, {})
        pass_count = int(row.get("pass_count") or 0)
        fail_count = int(row.get("fail_count") or 0)
        if not passes_default_mute(pass_count, fail_count, thresholds):
            candidates.append(full_name)
    return sorted(candidates)


def _chunk_list(items: List[str], size: int) -> List[List[str]]:
    if not items:
        return []
    return [items[i : i + size] for i in range(0, len(items), size)]


def _as_calendar_date(value: object) -> Optional[datetime.date]:
    if value is None:
        return None
    if isinstance(value, datetime.datetime):
        return value.date()
    if isinstance(value, datetime.date):
        return value
    dt = _to_utc_datetime(value)
    return dt.date() if dt else None


def _sanitize_mute_anchor_calendar(d: Optional[datetime.date]) -> Optional[datetime.date]:
    """Drop sentinel / unset dates (e.g. 1970-01-01) from tests_monitor mute_state_change_date."""
    if d is None or d.year < 2000:
        return None
    return d


def _fetch_monitor_mute_anchor_dates_bulk(
    ydb_wrapper: YDBWrapper,
    branch: str,
    build_type: str,
    full_names: List[str],
    anchor_calendar: datetime.date,
) -> Dict[str, Optional[datetime.date]]:
    """Per full_name: mute_state_change_date on latest date_window row <= anchor (batched IN)."""
    out: Dict[str, Optional[datetime.date]] = {n: None for n in full_names}
    if not full_names:
        return out
    tests_monitor_path = ydb_wrapper.get_table_path("tests_monitor")
    ad = anchor_calendar.isoformat()
    for chunk in _chunk_list(sorted(set(full_names)), 250):
        escaped = ", ".join("'" + n.replace("'", "''") + "'" for n in chunk)
        query = f"""
            $ranked = (
                SELECT
                    full_name,
                    mute_state_change_date,
                    ROW_NUMBER() OVER (
                        PARTITION BY full_name
                        ORDER BY date_window DESC
                    ) AS rn
                FROM `{tests_monitor_path}`
                WHERE branch = '{branch}'
                  AND build_type = '{build_type}'
                  AND full_name IN ({escaped})
                  AND date_window <= Date('{ad}')
            );

            SELECT full_name, mute_state_change_date
            FROM $ranked
            WHERE rn = 1
        """
        rows = ydb_wrapper.execute_scan_query(
            query,
            query_name=f"mute_coordinator_monitor_mute_anchor_bulk_{branch}_{build_type}",
        )
        for row in rows:
            fn = row.get("full_name")
            if not fn:
                continue
            out[str(fn)] = _sanitize_mute_anchor_calendar(
                _as_calendar_date(row.get("mute_state_change_date"))
            )
    return out


def _fetch_mute_window_pass_fail_bulk(
    ydb_wrapper: YDBWrapper,
    branch: str,
    build_type: str,
    full_names: List[str],
    end_date: datetime.date,
    window_days: int,
) -> Dict[str, Tuple[int, int]]:
    """Sum pass/fail per full_name over [end_date - (window_days-1), end_date] inclusive."""
    out: Dict[str, Tuple[int, int]] = {}
    if not full_names:
        return out
    tests_monitor_path = ydb_wrapper.get_table_path("tests_monitor")
    w = max(int(window_days), 1)
    span = max(w - 1, 0)
    end_d = end_date.isoformat()
    for chunk in _chunk_list(sorted(set(full_names)), 250):
        escaped = ", ".join("'" + n.replace("'", "''") + "'" for n in chunk)
        query = f"""
            SELECT
                full_name,
                SUM(COALESCE(pass_count, 0)) AS pass_count,
                SUM(COALESCE(fail_count, 0)) AS fail_count
            FROM `{tests_monitor_path}`
            WHERE branch = '{branch}'
              AND build_type = '{build_type}'
              AND full_name IN ({escaped})
              AND date_window >= Date('{end_d}') - {span}*Interval("P1D")
              AND date_window <= Date('{end_d}')
            GROUP BY full_name
        """
        rows = ydb_wrapper.execute_scan_query(
            query,
            query_name=f"mute_coordinator_mute_window_bulk_{branch}_{build_type}",
        )
        for row in rows:
            fn = row.get("full_name")
            if not fn:
                continue
            out[str(fn)] = (
                int(row.get("pass_count") or 0),
                int(row.get("fail_count") or 0),
            )
    return out


def _fetch_quarantine_stats(
    ydb_wrapper: YDBWrapper,
    branch: str,
    build_type: str,
    full_names: List[str],
    window_days: int,
    as_of_date: Optional[datetime.date] = None,
) -> Dict[str, Dict[str, int]]:
    if not full_names:
        return {}
    tests_monitor_path = ydb_wrapper.get_table_path("tests_monitor")
    anchor_date = _date_expr(as_of_date)
    escaped = ", ".join("'" + name.replace("'", "''") + "'" for name in sorted(set(full_names)))
    query = f"""
        SELECT
            full_name,
            SUM(COALESCE(pass_count, 0)) AS pass_count,
            SUM(COALESCE(fail_count, 0)) AS fail_count,
            SUM(COALESCE(mute_count, 0)) AS mute_count
        FROM `{tests_monitor_path}`
        WHERE branch = '{branch}'
          AND build_type = '{build_type}'
          AND date_window >= {anchor_date} - {int(window_days) - 1}*Interval("P1D")
          AND date_window <= {anchor_date}
          AND full_name IN ({escaped})
        GROUP BY full_name
    """
    rows = ydb_wrapper.execute_scan_query(
        query,
        query_name=f"mute_coordinator_quarantine_stats_{branch}_{build_type}_{window_days}",
    )
    out: Dict[str, Dict[str, int]] = {}
    for row in rows:
        full_name = row.get("full_name")
        if not full_name:
            continue
        out[str(full_name)] = {
            "pass_count": int(row.get("pass_count") or 0),
            "fail_count": int(row.get("fail_count") or 0),
            "mute_count": int(row.get("mute_count") or 0),
        }
    return out


def _fetch_deleted_test_signals(
    ydb_wrapper: YDBWrapper,
    *,
    branch: str,
    build_type: str,
    full_names: List[str],
) -> Dict[str, Dict[str, object]]:
    """
    Placeholder for future explicit "test deleted" signal source.

    Expected future return shape:
    {
        "<full_name>": {
            "signal_source": "test_registry|owners_diff|ci_event",
            "signal_at": "<timestamp-or-date>",
            "details": {...}
        }
    }
    """
    _ = (ydb_wrapper, branch, build_type, full_names)
    return {}


def _event_id(seed: str) -> str:
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()[:32]


def _build_state_row(
    *,
    branch: str,
    build_type: str,
    full_name: str,
    issue_number: int,
    lifecycle_state: str,
    policy_type: str,
    policy_snapshot: str,
    request_source: str,
    now: datetime.datetime,
    state_entered_at: Optional[datetime.datetime] = None,
    state_until: Optional[datetime.datetime] = None,
    decision_reason: Optional[str] = None,
) -> Dict[str, object]:
    return {
        "branch": branch,
        "build_type": build_type,
        "full_name": full_name,
        "issue_number": int(issue_number),
        "lifecycle_state": lifecycle_state,
        "policy_type": policy_type,
        "policy_version": "v1",
        "policy_params_json": policy_snapshot,
        "request_source": request_source,
        "requested_by_login": None,
        "requested_by_type": "system",
        "decision_reason": decision_reason,
        "state_entered_at": state_entered_at or now,
        "state_until": state_until,
        "last_evaluated_at": now,
        "active": 1,
        "updated_at": now,
    }


def _build_effective_row(
    *,
    branch: str,
    build_type: str,
    full_name: str,
    issue_number: int,
    lifecycle_state: str,
    policy_type: str,
    thresholds: Dict[str, int],
    now: datetime.datetime,
    state_entered_at: Optional[datetime.datetime] = None,
    state_until: Optional[datetime.datetime] = None,
) -> Dict[str, object]:
    min_runs = (
        int(thresholds["quarantine_min_runs"])
        if policy_type.startswith("quarantine_")
        else int(thresholds["default_unmute_min_runs"])
    )
    return {
        "branch": branch,
        "build_type": build_type,
        "full_name": full_name,
        "issue_number": int(issue_number),
        "effective_rule_type": policy_type,
        "effective_window_days": _rule_window_days(policy_type, thresholds),
        "effective_min_runs": min_runs,
        "effective_fail_limit": 0,
        "rule_source_state": lifecycle_state,
        "rule_valid_from": state_entered_at or now,
        "rule_valid_until": state_until,
        "computed_at": now,
    }


def _is_expired_quarantine(row: Dict[str, object], now: datetime.datetime) -> bool:
    policy_type = str(row.get("policy_type") or "")
    if not policy_type.startswith("quarantine_"):
        return False
    state_until = _to_utc_datetime(row.get("state_until"))
    if state_until is None:
        return False
    return state_until <= now


def _validate_single_active_rule_invariant(
    state_map: Dict[str, Dict[str, object]],
    rule_map: Dict[str, Dict[str, object]],
    *,
    branch: str,
    build_type: str,
) -> None:
    state_names = set(state_map.keys())
    rule_names = set(rule_map.keys())
    if state_names != rule_names:
        missing_in_rules = sorted(state_names - rule_names)
        missing_in_states = sorted(rule_names - state_names)
        raise RuntimeError(
            "Invariant violation: state/rule key mismatch "
            f"(branch={branch}, build_type={build_type}, "
            f"missing_in_rules={len(missing_in_rules)}, missing_in_states={len(missing_in_states)})"
        )

    for full_name, state_row in state_map.items():
        lifecycle_state = str(state_row.get("lifecycle_state") or "")
        if lifecycle_state not in _LIFECYCLE_COARSE:
            raise RuntimeError(
                "Invariant violation: lifecycle_state must be one of "
                f"{sorted(_LIFECYCLE_COARSE)} for {full_name} "
                f"(branch={branch}, build_type={build_type}, lifecycle_state={lifecycle_state!r})"
            )
        policy_type = str(state_row.get("policy_type") or "")
        if policy_type not in VALID_POLICY_TYPES:
            raise RuntimeError(
                "Invariant violation: unsupported policy type "
                f"for {full_name} (branch={branch}, build_type={build_type}, policy_type={policy_type})"
            )

    for full_name, rule_row in rule_map.items():
        effective_rule_type = str(rule_row.get("effective_rule_type") or "")
        if effective_rule_type not in VALID_POLICY_TYPES:
            raise RuntimeError(
                "Invariant violation: unsupported effective rule type "
                f"for {full_name} (branch={branch}, build_type={build_type}, "
                f"effective_rule_type={effective_rule_type})"
            )


def sync_effective_rules(
    branch: str,
    build_type: str,
    as_of_date: Optional[datetime.date] = None,
) -> int:
    thresholds = _load_thresholds()
    if as_of_date is None:
        now = datetime.datetime.now(datetime.timezone.utc)
    else:
        now = datetime.datetime.combine(
            as_of_date,
            datetime.time(12, 0, tzinfo=datetime.timezone.utc),
        )
    policy_snapshot = json.dumps(thresholds, ensure_ascii=True)

    with YDBWrapper() as ydb_wrapper:
        if not ydb_wrapper.check_credentials():
            return 1

        paths = _create_tables(ydb_wrapper)
        muted_tests = _fetch_current_muted_tests(
            ydb_wrapper,
            branch,
            build_type,
            as_of_date=as_of_date,
        )
        existing_state = _fetch_existing_control_state(
            ydb_wrapper,
            paths["control_state"],
            branch,
            build_type,
        )
        manual_quarantine_candidates = _fetch_manual_unmute_candidates(
            ydb_wrapper,
            branch=branch,
            build_type=build_type,
            thresholds=thresholds,
            as_of_date=as_of_date,
        )
        manual_mute_quarantine_candidates = _fetch_manual_mute_candidates(
            ydb_wrapper,
            branch=branch,
            build_type=build_type,
            thresholds=thresholds,
            as_of_date=as_of_date,
        )
        new_test_candidates = _fetch_new_test_candidates(
            ydb_wrapper,
            branch=branch,
            build_type=build_type,
            as_of_date=as_of_date,
        )
        user_fixed_candidates = _fetch_user_fixed_candidates(
            ydb_wrapper,
            branch=branch,
            build_type=build_type,
            muted_tests=muted_tests,
            as_of_date=as_of_date,
        )

        state_map: Dict[str, Dict[str, object]] = {}
        rule_map: Dict[str, Dict[str, object]] = {}
        event_rows: List[Dict[str, object]] = []

        # 1) Baseline for currently muted tests.
        for full_name in muted_tests:
            previous = existing_state.get(full_name)
            if previous:
                policy_type = str(previous.get("policy_type") or "default_unmute")
                lifecycle_state = str(previous.get("lifecycle_state") or LS_MUTED)
                state_entered_at = previous.get("state_entered_at") or now
                state_until = previous.get("state_until")
                issue_number = int(previous.get("issue_number") or 0)
                request_source = str(previous.get("request_source") or "system_transition")
                decision_reason = previous.get("decision_reason")
            else:
                policy_type = "default_unmute"
                lifecycle_state = LS_MUTED
                state_entered_at = now
                state_until = None
                issue_number = 0
                request_source = "system_default_sync"
                decision_reason = None

            state_map[full_name] = _build_state_row(
                branch=branch,
                build_type=build_type,
                full_name=full_name,
                issue_number=issue_number,
                lifecycle_state=lifecycle_state,
                policy_type=policy_type,
                policy_snapshot=policy_snapshot,
                request_source=request_source,
                decision_reason=str(decision_reason) if decision_reason else None,
                now=now,
                state_entered_at=state_entered_at,
                state_until=state_until,
            )
            rule_map[full_name] = _build_effective_row(
                branch=branch,
                build_type=build_type,
                full_name=full_name,
                issue_number=issue_number,
                lifecycle_state=lifecycle_state,
                policy_type=policy_type,
                thresholds=thresholds,
                now=now,
                state_entered_at=state_entered_at,
                state_until=state_until,
            )

        # 2) quarantine_new_test.
        new_window_days = int(thresholds["quarantine_new_test_window_days"])
        for full_name in new_test_candidates:
            if full_name in state_map:
                continue
            state_until = now + datetime.timedelta(days=new_window_days)
            state_map[full_name] = _build_state_row(
                branch=branch,
                build_type=build_type,
                full_name=full_name,
                issue_number=0,
                lifecycle_state=LS_QUARANTINE,
                policy_type="quarantine_new_test",
                policy_snapshot=policy_snapshot,
                request_source="auto_new_test_detected",
                now=now,
                state_entered_at=now,
                state_until=state_until,
            )
            rule_map[full_name] = _build_effective_row(
                branch=branch,
                build_type=build_type,
                full_name=full_name,
                issue_number=0,
                lifecycle_state=LS_QUARANTINE,
                policy_type="quarantine_new_test",
                thresholds=thresholds,
                now=now,
                state_entered_at=now,
                state_until=state_until,
            )
            event_rows.append(
                {
                    "event_date": now.date(),
                    "event_id": _event_id(
                        f"new_test_quarantine_started:{branch}:{build_type}:{full_name}:{now.date().isoformat()}"
                    ),
                    "event_time": now,
                    "branch": branch,
                    "build_type": build_type,
                    "full_name": full_name,
                    "issue_number": 0,
                    "event_type": "new_test_quarantine_started",
                    "before_state": "none",
                    "after_state": LS_QUARANTINE,
                    "before_policy": "none",
                    "after_policy": "quarantine_new_test",
                    "actor_login": None,
                    "actor_type": "system",
                    "payload_json": json.dumps(
                        {"quarantine_days": new_window_days},
                        ensure_ascii=True,
                    ),
                }
            )

        # 3) quarantine_user_fixed.
        user_window_days = int(thresholds["quarantine_user_fixed_window_days"])
        for full_name, issue_number in user_fixed_candidates.items():
            previous = state_map.get(full_name) or existing_state.get(full_name)
            if previous and str(previous.get("policy_type")) == "quarantine_user_fixed":
                continue
            state_until = now + datetime.timedelta(days=user_window_days)
            state_map[full_name] = _build_state_row(
                branch=branch,
                build_type=build_type,
                full_name=full_name,
                issue_number=issue_number,
                lifecycle_state=LS_QUARANTINE,
                policy_type="quarantine_user_fixed",
                policy_snapshot=policy_snapshot,
                request_source="issue_closed_request",
                now=now,
                state_entered_at=now,
                state_until=state_until,
            )
            rule_map[full_name] = _build_effective_row(
                branch=branch,
                build_type=build_type,
                full_name=full_name,
                issue_number=issue_number,
                lifecycle_state=LS_QUARANTINE,
                policy_type="quarantine_user_fixed",
                thresholds=thresholds,
                now=now,
                state_entered_at=now,
                state_until=state_until,
            )
            event_rows.append(
                {
                    "event_date": now.date(),
                    "event_id": _event_id(
                        f"user_fixed_quarantine_started:{branch}:{build_type}:{full_name}:{issue_number}:{now.date().isoformat()}"
                    ),
                    "event_time": now,
                    "branch": branch,
                    "build_type": build_type,
                    "full_name": full_name,
                    "issue_number": int(issue_number),
                    "event_type": "user_fixed_quarantine_started",
                    "before_state": str((previous or {}).get("lifecycle_state") or LS_MUTED),
                    "after_state": LS_QUARANTINE,
                    "before_policy": str((previous or {}).get("policy_type") or "default_unmute"),
                    "after_policy": "quarantine_user_fixed",
                    "actor_login": None,
                    "actor_type": "system",
                    "payload_json": json.dumps(
                        {"quarantine_days": user_window_days},
                        ensure_ascii=True,
                    ),
                }
            )

        # 4) quarantine_manual_unmute.
        manual_window_days = int(thresholds["quarantine_manual_unmute_window_days"])
        for full_name in manual_quarantine_candidates:
            previous = state_map.get(full_name) or existing_state.get(full_name)
            if previous and str(previous.get("policy_type")) == "quarantine_manual_unmute":
                continue
            state_until = now + datetime.timedelta(days=manual_window_days)
            state_map[full_name] = _build_state_row(
                branch=branch,
                build_type=build_type,
                full_name=full_name,
                issue_number=int((previous or {}).get("issue_number") or 0),
                lifecycle_state=LS_QUARANTINE,
                policy_type="quarantine_manual_unmute",
                policy_snapshot=policy_snapshot,
                request_source="manual_unmute_without_default_criteria",
                now=now,
                state_entered_at=now,
                state_until=state_until,
            )
            rule_map[full_name] = _build_effective_row(
                branch=branch,
                build_type=build_type,
                full_name=full_name,
                issue_number=int((previous or {}).get("issue_number") or 0),
                lifecycle_state=LS_QUARANTINE,
                policy_type="quarantine_manual_unmute",
                thresholds=thresholds,
                now=now,
                state_entered_at=now,
                state_until=state_until,
            )
            event_rows.append(
                {
                    "event_date": now.date(),
                    "event_id": _event_id(
                        f"manual_unmute_quarantine_started:{branch}:{build_type}:{full_name}:{now.date().isoformat()}"
                    ),
                    "event_time": now,
                    "branch": branch,
                    "build_type": build_type,
                    "full_name": full_name,
                    "issue_number": int((previous or {}).get("issue_number") or 0),
                    "event_type": "manual_unmute_quarantine_started",
                    "before_state": str((previous or {}).get("lifecycle_state") or LS_UNMUTED),
                    "after_state": LS_QUARANTINE,
                    "before_policy": str((previous or {}).get("policy_type") or "default_unmute"),
                    "after_policy": "quarantine_manual_unmute",
                    "actor_login": None,
                    "actor_type": "system",
                    "payload_json": json.dumps(
                        {
                            "reason": "manual_unmute_without_default_criteria",
                            "quarantine_days": manual_window_days,
                        },
                        ensure_ascii=True,
                    ),
                }
            )

        # 5) quarantine_manual_mute.
        manual_mute_window_days = int(thresholds["quarantine_manual_mute_window_days"])
        for full_name in manual_mute_quarantine_candidates:
            previous = state_map.get(full_name) or existing_state.get(full_name)
            previous_policy = str((previous or {}).get("policy_type") or "default_unmute")
            if previous_policy.startswith("quarantine_"):
                continue
            state_until = now + datetime.timedelta(days=manual_mute_window_days)
            state_map[full_name] = _build_state_row(
                branch=branch,
                build_type=build_type,
                full_name=full_name,
                issue_number=int((previous or {}).get("issue_number") or 0),
                lifecycle_state=LS_QUARANTINE,
                policy_type="quarantine_manual_mute",
                policy_snapshot=policy_snapshot,
                request_source="manual_mute_without_default_criteria",
                now=now,
                state_entered_at=now,
                state_until=state_until,
            )
            rule_map[full_name] = _build_effective_row(
                branch=branch,
                build_type=build_type,
                full_name=full_name,
                issue_number=int((previous or {}).get("issue_number") or 0),
                lifecycle_state=LS_QUARANTINE,
                policy_type="quarantine_manual_mute",
                thresholds=thresholds,
                now=now,
                state_entered_at=now,
                state_until=state_until,
            )
            event_rows.append(
                {
                    "event_date": now.date(),
                    "event_id": _event_id(
                        f"manual_mute_quarantine_started:{branch}:{build_type}:{full_name}:{now.date().isoformat()}"
                    ),
                    "event_time": now,
                    "branch": branch,
                    "build_type": build_type,
                    "full_name": full_name,
                    "issue_number": int((previous or {}).get("issue_number") or 0),
                    "event_type": "manual_mute_quarantine_started",
                    "before_state": str((previous or {}).get("lifecycle_state") or LS_UNMUTED),
                    "after_state": LS_QUARANTINE,
                    "before_policy": previous_policy,
                    "after_policy": "quarantine_manual_mute",
                    "actor_login": None,
                    "actor_type": "system",
                    "payload_json": json.dumps(
                        {
                            "reason": "manual_mute_without_default_criteria",
                            "quarantine_days": manual_mute_window_days,
                        },
                        ensure_ascii=True,
                    ),
                }
            )

        # 5.5) Reconcile active manual-mute quarantine: re-check default mute criteria on the window
        # ending on the mute flip day from tests_monitor (mute_state_change_date), not only
        # control_state.state_entered_at (sync time can drift from the monitor mute anchor).
        # Batched YQL: one anchor scan per chunk of tests, then one pass/fail scan per distinct end_date.
        anchor_cal = as_of_date if as_of_date is not None else now.date()
        reconcile_targets: List[Tuple[str, Dict[str, object], datetime.date]] = []
        for full_name, row in list(state_map.items()):
            if str(row.get("policy_type") or "") != "quarantine_manual_mute":
                continue
            if str(row.get("lifecycle_state") or "") != LS_QUARANTINE:
                continue
            entered = _to_utc_datetime(row.get("state_entered_at")) or now
            reconcile_targets.append((full_name, row, entered.date()))

        if reconcile_targets:
            mw = int(thresholds["default_mute_window_days"])
            all_names = sorted({t[0] for t in reconcile_targets})
            anchors: Dict[str, Optional[datetime.date]] = {}
            for chunk in _chunk_list(all_names, 250):
                anchors.update(
                    _fetch_monitor_mute_anchor_dates_bulk(
                        ydb_wrapper, branch, build_type, chunk, anchor_cal
                    )
                )
            end_to_names: Dict[datetime.date, List[str]] = {}
            for full_name, _row, entered_date in reconcile_targets:
                monitor_anchor = anchors.get(full_name)
                end_date = min((monitor_anchor or entered_date), anchor_cal)
                end_to_names.setdefault(end_date, []).append(full_name)

            stats_by_name: Dict[str, Tuple[int, int]] = {}
            for end_date, names in end_to_names.items():
                for chunk in _chunk_list(sorted(set(names)), 250):
                    stats_by_name.update(
                        _fetch_mute_window_pass_fail_bulk(
                            ydb_wrapper, branch, build_type, chunk, end_date, mw
                        )
                    )

            for full_name, row, entered_date in reconcile_targets:
                monitor_anchor = anchors.get(full_name)
                end_date = min((monitor_anchor or entered_date), anchor_cal)
                pc, fc = stats_by_name.get(full_name, (0, 0))
                if not passes_default_mute(pc, fc, thresholds):
                    continue
                issue_number = int(row.get("issue_number") or 0)
                state_map[full_name] = _build_state_row(
                    branch=branch,
                    build_type=build_type,
                    full_name=full_name,
                    issue_number=issue_number,
                    lifecycle_state=LS_MUTED,
                    policy_type="default_unmute",
                    policy_snapshot=policy_snapshot,
                    request_source="reconcile_manual_mute_quarantine",
                    now=now,
                    state_entered_at=row.get("state_entered_at") or now,
                    state_until=None,
                    decision_reason="default_mute_criteria_met_on_mute_window",
                )
                rule_map[full_name] = _build_effective_row(
                    branch=branch,
                    build_type=build_type,
                    full_name=full_name,
                    issue_number=issue_number,
                    lifecycle_state=LS_MUTED,
                    policy_type="default_unmute",
                    thresholds=thresholds,
                    now=now,
                    state_entered_at=row.get("state_entered_at") or now,
                    state_until=None,
                )
                event_rows.append(
                    {
                        "event_date": now.date(),
                        "event_id": _event_id(
                            f"manual_mute_quarantine_reconciled:{branch}:{build_type}:{full_name}:{now.date().isoformat()}"
                        ),
                        "event_time": now,
                        "branch": branch,
                        "build_type": build_type,
                        "full_name": full_name,
                        "issue_number": issue_number,
                        "event_type": "manual_mute_quarantine_reconciled",
                        "before_state": LS_QUARANTINE,
                        "after_state": LS_MUTED,
                        "before_policy": "quarantine_manual_mute",
                        "after_policy": "default_unmute",
                        "actor_login": None,
                        "actor_type": "system",
                        "payload_json": json.dumps(
                            {
                                "reason": "default_mute_criteria_met_on_mute_window",
                                "pass_count": pc,
                                "fail_count": fc,
                                "window_days": mw,
                                "window_end_date": end_date.isoformat(),
                            },
                            ensure_ascii=True,
                        ),
                    }
                )

        # 6) Resolve expired quarantines.
        expired_quarantine = {
            full_name: row
            for full_name, row in state_map.items()
            if _is_expired_quarantine(row, now)
        }
        for full_name, row in existing_state.items():
            if full_name in expired_quarantine:
                continue
            if _is_expired_quarantine(row, now):
                expired_quarantine[full_name] = row

        if expired_quarantine:
            grouped: Dict[int, List[str]] = {}
            for full_name, row in expired_quarantine.items():
                window_days = _rule_window_days(str(row.get("policy_type") or "default_unmute"), thresholds)
                grouped.setdefault(window_days, []).append(full_name)
            stats_by_test: Dict[str, Dict[str, int]] = {}
            for window_days, names in grouped.items():
                stats_by_test.update(
                    _fetch_quarantine_stats(
                        ydb_wrapper,
                        branch=branch,
                        build_type=build_type,
                        full_names=names,
                        window_days=window_days,
                        as_of_date=as_of_date,
                    )
                )
            deleted_signals = _fetch_deleted_test_signals(
                ydb_wrapper,
                branch=branch,
                build_type=build_type,
                full_names=list(expired_quarantine.keys()),
            )
            for full_name, old_row in expired_quarantine.items():
                stats = stats_by_test.get(full_name, {"pass_count": 0, "fail_count": 0, "mute_count": 0})
                pass_count = int(stats["pass_count"])
                fail_count = int(stats["fail_count"])
                mute_count = int(stats["mute_count"])
                runs = pass_count + fail_count + mute_count
                if runs == 0:
                    new_state = LS_UNMUTED
                    reason = "no_runs_after_quarantine"
                    if full_name in deleted_signals:
                        reason = "test_deleted_confirmed"
                else:
                    is_stable = runs >= int(thresholds["quarantine_min_runs"]) and (fail_count + mute_count == 0)
                    new_state = LS_UNMUTED if is_stable else LS_MUTED
                    reason = "stable_window_passed" if is_stable else "ttl_expired_not_stable"
                issue_number = int(old_row.get("issue_number") or 0)
                state_map[full_name] = _build_state_row(
                    branch=branch,
                    build_type=build_type,
                    full_name=full_name,
                    issue_number=issue_number,
                    lifecycle_state=new_state,
                    policy_type="default_unmute",
                    policy_snapshot=policy_snapshot,
                    request_source="quarantine_resolver",
                    now=now,
                    state_entered_at=now,
                    state_until=None,
                    decision_reason=reason,
                )
                rule_map[full_name] = _build_effective_row(
                    branch=branch,
                    build_type=build_type,
                    full_name=full_name,
                    issue_number=issue_number,
                    lifecycle_state=new_state,
                    policy_type="default_unmute",
                    thresholds=thresholds,
                    now=now,
                    state_entered_at=now,
                    state_until=None,
                )
                event_rows.append(
                    {
                        "event_date": now.date(),
                        "event_id": _event_id(
                            f"quarantine_resolved:{branch}:{build_type}:{full_name}:{reason}:{now.date().isoformat()}"
                        ),
                        "event_time": now,
                        "branch": branch,
                        "build_type": build_type,
                        "full_name": full_name,
                        "issue_number": issue_number,
                        "event_type": "quarantine_resolved",
                        "before_state": str(old_row.get("lifecycle_state") or ""),
                        "after_state": new_state,
                        "before_policy": str(old_row.get("policy_type") or ""),
                        "after_policy": "default_unmute",
                        "actor_login": None,
                        "actor_type": "system",
                        "payload_json": json.dumps(
                            {
                                "decision_reason": reason,
                                "pass_count": pass_count,
                                "fail_count": fail_count,
                                "mute_count": mute_count,
                                "runs": runs,
                            },
                            ensure_ascii=True,
                        ),
                    }
                )

        # 7) Preserve active existing quarantine rows that are outside current muted set.
        for full_name, row in existing_state.items():
            if full_name in state_map:
                continue
            policy_type = str(row.get("policy_type") or "default_unmute")
            lifecycle_state = str(row.get("lifecycle_state") or LS_MUTED)
            if not policy_type.startswith("quarantine_"):
                continue
            if _is_expired_quarantine(row, now):
                continue
            issue_number = int(row.get("issue_number") or 0)
            state_entered_at = row.get("state_entered_at") or now
            state_until = row.get("state_until")
            state_map[full_name] = _build_state_row(
                branch=branch,
                build_type=build_type,
                full_name=full_name,
                issue_number=issue_number,
                lifecycle_state=lifecycle_state,
                policy_type=policy_type,
                policy_snapshot=policy_snapshot,
                request_source=str(row.get("request_source") or "system_preserve"),
                now=now,
                state_entered_at=state_entered_at,
                state_until=state_until,
                decision_reason=str(row.get("decision_reason") or "") or None,
            )
            rule_map[full_name] = _build_effective_row(
                branch=branch,
                build_type=build_type,
                full_name=full_name,
                issue_number=issue_number,
                lifecycle_state=lifecycle_state,
                policy_type=policy_type,
                thresholds=thresholds,
                now=now,
                state_entered_at=state_entered_at,
                state_until=state_until,
            )

        effective_rows = list(rule_map.values())
        state_rows = list(state_map.values())
        _validate_single_active_rule_invariant(
            state_map,
            rule_map,
            branch=branch,
            build_type=build_type,
        )

        if not state_rows:
            print(
                "No mute coordinator rows found for "
                f"branch={branch}, build_type={build_type}"
            )
            return 0

        ydb_wrapper.bulk_upsert_batches(
            paths["effective_rule"],
            effective_rows,
            _build_column_types_for_effective_rule(),
            batch_size=1000,
            query_name=f"mute_coordinator_effective_rule_{branch}_{build_type}",
        )
        ydb_wrapper.bulk_upsert_batches(
            paths["control_state"],
            state_rows,
            _build_column_types_for_control_state(),
            batch_size=1000,
            query_name=f"mute_coordinator_control_state_{branch}_{build_type}",
        )
        if event_rows:
            ydb_wrapper.bulk_upsert_batches(
                paths["control_events"],
                event_rows,
                _build_column_types_for_control_events(),
                batch_size=500,
                query_name=f"mute_coordinator_control_events_{branch}_{build_type}",
            )

        print(
            "Mute coordinator sync complete: "
            f"branch={branch}, build_type={build_type}, as_of_date={(as_of_date.isoformat() if as_of_date else 'current')}, "
            f"muted_tests={len(muted_tests)}, "
            f"new_test_candidates={len(new_test_candidates)}, "
            f"user_fixed_candidates={len(user_fixed_candidates)}, "
            f"manual_quarantine_candidates={len(manual_quarantine_candidates)}, "
            f"manual_mute_quarantine_candidates={len(manual_mute_quarantine_candidates)}, "
            f"preserved_existing={len(existing_state)}, events={len(event_rows)}"
        )
    return 0


def bootstrap_only() -> int:
    with YDBWrapper() as ydb_wrapper:
        if not ydb_wrapper.check_credentials():
            return 1
        _create_tables(ydb_wrapper)
    print("Mute coordinator tables are ready.")
    return 0


def run_backfill(
    *,
    branches: List[str],
    build_types: List[str],
    backfill_days: int,
    end_date: Optional[datetime.date],
) -> int:
    if backfill_days <= 0:
        raise ValueError("backfill_days must be > 0")
    end = end_date or datetime.datetime.now(datetime.timezone.utc).date()
    start = end - datetime.timedelta(days=backfill_days - 1)
    print(
        "Starting mute coordinator backfill: "
        f"start={start.isoformat()}, end={end.isoformat()}, "
        f"branches={branches}, build_types={build_types}"
    )
    for day_offset in range(backfill_days):
        as_of_date = start + datetime.timedelta(days=day_offset)
        for branch in branches:
            for build_type in build_types:
                print(
                    "Backfill tick: "
                    f"date={as_of_date.isoformat()}, branch={branch}, build_type={build_type}"
                )
                rc = sync_effective_rules(
                    branch=branch,
                    build_type=build_type,
                    as_of_date=as_of_date,
                )
                if rc != 0:
                    return rc
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Export mute coordinator state into YDB")
    parser.add_argument(
        "command",
        choices=["bootstrap", "sync-effective-rules", "backfill"],
        nargs="?",
        default="sync-effective-rules",
        help="Command to execute",
    )
    parser.add_argument("--branch", default="main", help="Target branch")
    parser.add_argument("--build-type", dest="build_type", default="relwithdebinfo", help="Target build type")
    parser.add_argument(
        "--as-of-date",
        dest="as_of_date",
        default=None,
        help="Anchor date YYYY-MM-DD for sync-effective-rules",
    )
    parser.add_argument(
        "--branches",
        default="main",
        help="Comma-separated branches for backfill mode",
    )
    parser.add_argument(
        "--build-types",
        dest="build_types",
        default="relwithdebinfo",
        help="Comma-separated build types for backfill mode",
    )
    parser.add_argument(
        "--backfill-days",
        dest="backfill_days",
        type=int,
        default=30,
        help="Number of days for backfill mode",
    )
    parser.add_argument(
        "--backfill-end-date",
        dest="backfill_end_date",
        default=None,
        help="Backfill end date YYYY-MM-DD (default today UTC)",
    )
    args = parser.parse_args()

    if args.command == "bootstrap":
        return bootstrap_only()

    if args.command == "backfill":
        branches = [x.strip() for x in args.branches.split(",") if x.strip()]
        build_types = [x.strip() for x in args.build_types.split(",") if x.strip()]
        end_date = _parse_date(args.backfill_end_date) if args.backfill_end_date else None
        return run_backfill(
            branches=branches,
            build_types=build_types,
            backfill_days=args.backfill_days,
            end_date=end_date,
        )

    as_of_date = _parse_date(args.as_of_date) if args.as_of_date else None
    return sync_effective_rules(
        branch=args.branch,
        build_type=args.build_type,
        as_of_date=as_of_date,
    )


if __name__ == "__main__":
    raise SystemExit(main())
