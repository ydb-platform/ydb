#!/usr/bin/env python3

import argparse
import datetime
import json
import os
import hashlib
import sys
from typing import Dict, List, Optional, Tuple

import ydb

from ydb_wrapper import YDBWrapper

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from github_issue_utils import DEFAULT_BUILD_TYPE, parse_body


DEFAULT_CONFIG_REL_PATH = os.path.join("..", "..", "config", "mute_coordinator_thresholds.json")


def _load_thresholds() -> Dict[str, int]:
    base_dir = os.path.dirname(__file__)
    config_path = os.path.normpath(os.path.join(base_dir, DEFAULT_CONFIG_REL_PATH))
    with open(config_path, "r", encoding="utf-8") as f:
        payload = json.load(f)
    return {k: int(v) for k, v in payload.items()}


def _create_control_state_table(ydb_wrapper: YDBWrapper, table_path: str) -> None:
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS `{table_path}` (
            `branch` Utf8 NOT NULL,
            `build_type` Utf8 NOT NULL,
            `full_name` Utf8 NOT NULL,
            `issue_number` Uint64,
            `lifecycle_state` Utf8 NOT NULL,
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


def _fetch_current_muted_tests(ydb_wrapper: YDBWrapper, branch: str, build_type: str) -> List[str]:
    tests_monitor_path = ydb_wrapper.get_table_path("tests_monitor")
    query = f"""
        $latest_date = (
            SELECT MAX(date_window) AS d
            FROM `{tests_monitor_path}`
            WHERE branch = '{branch}' AND build_type = '{build_type}'
        );

        SELECT DISTINCT full_name
        FROM `{tests_monitor_path}`
        WHERE branch = '{branch}'
          AND build_type = '{build_type}'
          AND date_window = (SELECT d FROM $latest_date)
          AND is_muted = 1
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
) -> Dict[str, int]:
    if not muted_tests:
        return {}
    issues_path = ydb_wrapper.get_table_path("issues")
    query = f"""
        SELECT issue_number, body
        FROM `{issues_path}`
        WHERE state = 'CLOSED'
          AND closed_at IS NOT NULL
          AND Cast(closed_at AS Date) >= CurrentUtcDate() - 2*Interval("P1D")
    """
    rows = ydb_wrapper.execute_scan_query(
        query,
        query_name=f"mute_coordinator_user_fixed_candidates_{branch}_{build_type}",
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


def _fetch_new_test_candidates(ydb_wrapper: YDBWrapper, branch: str, build_type: str) -> List[str]:
    tests_monitor_path = ydb_wrapper.get_table_path("tests_monitor")
    query = f"""
        $latest_date = (
            SELECT MAX(date_window) AS d
            FROM `{tests_monitor_path}`
            WHERE branch = '{branch}' AND build_type = '{build_type}'
        );

        SELECT DISTINCT full_name
        FROM `{tests_monitor_path}`
        WHERE branch = '{branch}'
          AND build_type = '{build_type}'
          AND date_window = (SELECT d FROM $latest_date)
          AND is_muted = 0
          AND previous_state = 'no_runs'
          AND (COALESCE(pass_count, 0) + COALESCE(fail_count, 0) + COALESCE(mute_count, 0)) > 0
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
    default_min_runs: int,
) -> List[str]:
    tests_monitor_path = ydb_wrapper.get_table_path("tests_monitor")
    transition_query = f"""
        SELECT full_name
        FROM (
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
              AND date_window >= CurrentUtcDate() - 14*Interval("P1D")
        )
        GROUP BY full_name
        HAVING
            MAX(CASE WHEN rn = 1 THEN is_muted ELSE 0 END) = 0
            AND MAX(CASE WHEN rn = 2 THEN is_muted ELSE 0 END) = 1
    """
    transition_rows = ydb_wrapper.execute_scan_query(
        transition_query,
        query_name=f"mute_coordinator_manual_transition_{branch}_{build_type}",
    )
    transitioned = sorted(str(row["full_name"]) for row in transition_rows if row.get("full_name"))
    if not transitioned:
        return []

    escaped = ", ".join("'" + name.replace("'", "''") + "'" for name in transitioned)
    stats_query = f"""
        SELECT
            full_name,
            SUM(COALESCE(pass_count, 0)) AS pass_count,
            SUM(COALESCE(fail_count, 0)) AS fail_count,
            SUM(COALESCE(mute_count, 0)) AS mute_count
        FROM `{tests_monitor_path}`
        WHERE branch = '{branch}'
          AND build_type = '{build_type}'
          AND date_window >= CurrentUtcDate() - 6*Interval("P1D")
          AND full_name IN ({escaped})
        GROUP BY full_name
    """
    stats_rows = ydb_wrapper.execute_scan_query(
        stats_query,
        query_name=f"mute_coordinator_manual_candidate_stats_{branch}_{build_type}",
    )

    candidates: List[str] = []
    stats_by_name = {str(row["full_name"]): row for row in stats_rows if row.get("full_name")}
    for full_name in transitioned:
        row = stats_by_name.get(full_name, {})
        pass_count = int(row.get("pass_count") or 0)
        fail_count = int(row.get("fail_count") or 0)
        mute_count = int(row.get("mute_count") or 0)
        total_runs = pass_count + fail_count + mute_count
        passes_default_unmute = total_runs >= default_min_runs and (fail_count + mute_count == 0)
        if not passes_default_unmute:
            candidates.append(full_name)
    return sorted(candidates)


def _fetch_quarantine_stats(
    ydb_wrapper: YDBWrapper,
    branch: str,
    build_type: str,
    full_names: List[str],
    window_days: int,
) -> Dict[str, Dict[str, int]]:
    if not full_names:
        return {}
    tests_monitor_path = ydb_wrapper.get_table_path("tests_monitor")
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
          AND date_window >= CurrentUtcDate() - {int(window_days) - 1}*Interval("P1D")
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
    state_until = row.get("state_until")
    if state_until is None:
        return False
    if isinstance(state_until, datetime.datetime):
        return state_until <= now
    return False


def sync_effective_rules(branch: str, build_type: str) -> int:
    thresholds = _load_thresholds()
    now = datetime.datetime.now(datetime.timezone.utc)
    policy_snapshot = json.dumps(thresholds, ensure_ascii=True)

    with YDBWrapper() as ydb_wrapper:
        if not ydb_wrapper.check_credentials():
            return 1

        paths = _create_tables(ydb_wrapper)
        muted_tests = _fetch_current_muted_tests(ydb_wrapper, branch, build_type)
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
            default_min_runs=int(thresholds["default_unmute_min_runs"]),
        )
        new_test_candidates = _fetch_new_test_candidates(
            ydb_wrapper,
            branch=branch,
            build_type=build_type,
        )
        user_fixed_candidates = _fetch_user_fixed_candidates(
            ydb_wrapper,
            branch=branch,
            build_type=build_type,
            muted_tests=muted_tests,
        )

        state_map: Dict[str, Dict[str, object]] = {}
        rule_map: Dict[str, Dict[str, object]] = {}
        event_rows: List[Dict[str, object]] = []

        # 1) Baseline for currently muted tests.
        for full_name in muted_tests:
            previous = existing_state.get(full_name)
            if previous:
                policy_type = str(previous.get("policy_type") or "default_unmute")
                lifecycle_state = str(previous.get("lifecycle_state") or "muted_active")
                state_entered_at = previous.get("state_entered_at") or now
                state_until = previous.get("state_until")
                issue_number = int(previous.get("issue_number") or 0)
                request_source = str(previous.get("request_source") or "system_transition")
                decision_reason = previous.get("decision_reason")
            else:
                policy_type = "default_unmute"
                lifecycle_state = "muted_active"
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
                lifecycle_state="quarantine_new_active",
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
                lifecycle_state="quarantine_new_active",
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
                    "after_state": "quarantine_new_active",
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
                lifecycle_state="quarantine_user_active",
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
                lifecycle_state="quarantine_user_active",
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
                    "before_state": str((previous or {}).get("lifecycle_state") or "muted_active"),
                    "after_state": "quarantine_user_active",
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
                lifecycle_state="quarantine_manual_active",
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
                lifecycle_state="quarantine_manual_active",
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
                    "before_state": str((previous or {}).get("lifecycle_state") or "unmuted"),
                    "after_state": "quarantine_manual_active",
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

        # 5) Resolve expired quarantines.
        expired_quarantine = {
            full_name: row
            for full_name, row in state_map.items()
            if _is_expired_quarantine(row, now)
        }
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
                    )
                )
            for full_name, old_row in expired_quarantine.items():
                stats = stats_by_test.get(full_name, {"pass_count": 0, "fail_count": 0, "mute_count": 0})
                pass_count = int(stats["pass_count"])
                fail_count = int(stats["fail_count"])
                mute_count = int(stats["mute_count"])
                runs = pass_count + fail_count + mute_count
                is_stable = runs >= int(thresholds["quarantine_min_runs"]) and (fail_count + mute_count == 0)
                new_state = "unmuted" if is_stable else "muted_active"
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

        # 6) Preserve active existing quarantine rows that are outside current muted set.
        for full_name, row in existing_state.items():
            if full_name in state_map:
                continue
            policy_type = str(row.get("policy_type") or "default_unmute")
            lifecycle_state = str(row.get("lifecycle_state") or "muted_active")
            if not policy_type.startswith("quarantine_"):
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
            f"branch={branch}, build_type={build_type}, muted_tests={len(muted_tests)}, "
            f"new_test_candidates={len(new_test_candidates)}, "
            f"user_fixed_candidates={len(user_fixed_candidates)}, "
            f"manual_quarantine_candidates={len(manual_quarantine_candidates)}, "
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


def main() -> int:
    parser = argparse.ArgumentParser(description="Export mute coordinator state into YDB")
    parser.add_argument(
        "command",
        choices=["bootstrap", "sync-effective-rules"],
        nargs="?",
        default="sync-effective-rules",
        help="Command to execute",
    )
    parser.add_argument("--branch", default="main", help="Target branch")
    parser.add_argument("--build-type", dest="build_type", default="relwithdebinfo", help="Target build type")
    args = parser.parse_args()

    if args.command == "bootstrap":
        return bootstrap_only()
    return sync_effective_rules(branch=args.branch, build_type=args.build_type)


if __name__ == "__main__":
    raise SystemExit(main())
