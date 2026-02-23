#!/usr/bin/env python3
"""
Mute decisions history — stores all rule events: mute/unmute/delete/graduation/alert/log.

Usage:
  - create_new_muted_ya.py calls write_mute_decisions() after apply_and_add_mutes
  - evaluate_pr.py calls write_pattern_matches() for alert/log
  - Data written to test_results/analytics/mute_decisions
"""

import datetime
import json
import logging
import os
import sys
from typing import Any, Dict, List, Optional, Set

import ydb

# Add analytics for ydb_wrapper
_scripts = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_analytics = os.path.join(_scripts, 'analytics')
if _analytics not in sys.path:
    sys.path.insert(0, _analytics)
from ydb_wrapper import YDBWrapper


# Optional columns added later — table may not have them. We fallback to base if write fails.
_EXTRA_COLUMNS = ("match_details", "behavior_start_date", "behavior_start_commit", "behavior_start_pr")


def _mute_decisions_column_types() -> ydb.BulkUpsertColumns:
    """Full column types for mute_decisions table."""
    ct = ydb.BulkUpsertColumns()
    ct.add_column("timestamp", ydb.PrimitiveType.Timestamp)
    ct.add_column("full_name", ydb.PrimitiveType.Utf8)
    ct.add_column("build_type", ydb.PrimitiveType.Utf8)
    ct.add_column("branch", ydb.PrimitiveType.Utf8)
    ct.add_column("action", ydb.PrimitiveType.Utf8)
    ct.add_column("rule_id", ydb.OptionalType(ydb.PrimitiveType.Utf8))
    ct.add_column("reason", ydb.OptionalType(ydb.PrimitiveType.Utf8))
    ct.add_column("previous_state", ydb.OptionalType(ydb.PrimitiveType.Utf8))
    ct.add_column("new_state", ydb.OptionalType(ydb.PrimitiveType.Utf8))
    ct.add_column("match_details", ydb.OptionalType(ydb.PrimitiveType.Json))
    ct.add_column("behavior_start_date", ydb.OptionalType(ydb.PrimitiveType.Date))
    ct.add_column("behavior_start_commit", ydb.OptionalType(ydb.PrimitiveType.Utf8))
    ct.add_column("behavior_start_pr", ydb.OptionalType(ydb.PrimitiveType.Utf8))
    return ct


def _mute_decisions_column_types_base() -> ydb.BulkUpsertColumns:
    """Base columns only — for tables without match_details, behavior_start_*."""
    ct = ydb.BulkUpsertColumns()
    ct.add_column("timestamp", ydb.PrimitiveType.Timestamp)
    ct.add_column("full_name", ydb.PrimitiveType.Utf8)
    ct.add_column("build_type", ydb.PrimitiveType.Utf8)
    ct.add_column("branch", ydb.PrimitiveType.Utf8)
    ct.add_column("action", ydb.PrimitiveType.Utf8)
    ct.add_column("rule_id", ydb.OptionalType(ydb.PrimitiveType.Utf8))
    ct.add_column("reason", ydb.OptionalType(ydb.PrimitiveType.Utf8))
    ct.add_column("previous_state", ydb.OptionalType(ydb.PrimitiveType.Utf8))
    ct.add_column("new_state", ydb.OptionalType(ydb.PrimitiveType.Utf8))
    return ct


def _strip_extra_columns(rows: list) -> list:
    """Remove extra columns for fallback write to old schema."""
    return [{k: v for k, v in r.items() if k not in _EXTRA_COLUMNS} for r in rows]


def _test_line_to_full_name(line: str) -> str:
    """Convert 'suite_folder test_name' to 'suite_folder/test_name'."""
    parts = line.strip().split(" ", maxsplit=1)
    if len(parts) != 2:
        return line
    return f"{parts[0]}/{parts[1]}"


def create_mute_decisions_table(ydb_wrapper: YDBWrapper, table_path: str) -> None:
    """Create mute_decisions table if not exists."""
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS `{table_path}` (
            `timestamp` Timestamp NOT NULL,
            `full_name` Utf8 NOT NULL,
            `build_type` Utf8 NOT NULL,
            `branch` Utf8 NOT NULL,
            `action` Utf8 NOT NULL,
            `rule_id` Utf8,
            `reason` Utf8,
            `previous_state` Utf8,
            `new_state` Utf8,
            `match_details` Json,
            `behavior_start_date` Date,
            `behavior_start_commit` Utf8,
            `behavior_start_pr` Utf8,
            PRIMARY KEY (`timestamp`, `full_name`, `build_type`, `branch`, `action`)
        )
        PARTITION BY HASH(branch, build_type)
        WITH (STORE = COLUMN)
    """
    ydb_wrapper.create_table(table_path, create_sql)
    logging.info(f"mute_decisions table ready: {table_path}")


def write_mute_decisions(
    ydb_wrapper: YDBWrapper,
    branch: str,
    build_type: str,
    to_mute: List[str],
    to_unmute: List[str],
    to_delete: List[str],
    to_graduated: Set[str],
    mute_rule_id: str = "regression_flaky_mute",
    unmute_rule_id: str = "regression_stable_unmute",
    delete_rule_id: str = "regression_no_runs_delete",
    graduation_rule_id: str = "quarantine_graduation",
    to_mute_debug: Optional[List[str]] = None,
    to_unmute_debug: Optional[List[str]] = None,
    to_delete_debug: Optional[List[str]] = None,
    system_version: Optional[str] = None,
) -> int:
    """Write mute decisions to YDB for traceability."""
    to_mute_debug_map = dict(zip(to_mute, to_mute_debug)) if to_mute_debug and len(to_mute_debug) == len(to_mute) else {}
    to_unmute_debug_map = dict(zip(to_unmute, to_unmute_debug)) if to_unmute_debug and len(to_unmute_debug) == len(to_unmute) else {}
    to_delete_debug_map = dict(zip(to_delete, to_delete_debug)) if to_delete_debug and len(to_delete_debug) == len(to_delete) else {}

    table_path = ydb_wrapper.get_table_path("mute_decisions_v4" if system_version == "v4_direct" else "mute_decisions")
    create_mute_decisions_table(ydb_wrapper, table_path)

    now = datetime.datetime.now(datetime.timezone.utc)
    rows = []

    _empty_extra = {"match_details": None, "behavior_start_date": None, "behavior_start_commit": None, "behavior_start_pr": None}

    for line in to_mute:
        full_name = _test_line_to_full_name(line)
        reason = to_mute_debug_map.get(line, "")
        rows.append({
            "timestamp": now,
            "full_name": full_name,
            "build_type": build_type,
            "branch": branch,
            "action": "mute",
            "rule_id": mute_rule_id,
            "reason": reason,
            "previous_state": "unmuted",
            "new_state": "muted",
            **_empty_extra,
        })

    for line in to_unmute:
        full_name = _test_line_to_full_name(line)
        reason = to_unmute_debug_map.get(line, "")
        rows.append({
            "timestamp": now,
            "full_name": full_name,
            "build_type": build_type,
            "branch": branch,
            "action": "unmute",
            "rule_id": unmute_rule_id,
            "reason": reason,
            "previous_state": "muted",
            "new_state": "unmuted",
            **_empty_extra,
        })

    for line in to_delete:
        full_name = _test_line_to_full_name(line)
        reason = to_delete_debug_map.get(line, "")
        rows.append({
            "timestamp": now,
            "full_name": full_name,
            "build_type": build_type,
            "branch": branch,
            "action": "delete",
            "rule_id": delete_rule_id,
            "reason": reason,
            "previous_state": "muted",
            "new_state": "unmuted",
            **_empty_extra,
        })

    for line in to_graduated:
        full_name = _test_line_to_full_name(line)
        rows.append({
            "timestamp": now,
            "full_name": full_name,
            "build_type": build_type,
            "branch": branch,
            "action": "quarantine_graduation",
            "rule_id": graduation_rule_id,
            "reason": "4+ runs, 1+ pass in 1 day",
            "previous_state": "quarantine",
            "new_state": "unmuted",
            **_empty_extra,
        })

    if not rows:
        logging.info("No mute decisions to write")
        return 0

    try:
        ydb_wrapper.bulk_upsert(table_path, rows, _mute_decisions_column_types())
    except Exception as e:
        logging.warning(f"bulk_upsert with extra columns failed: {e}. Retrying with base schema.")
        ydb_wrapper.bulk_upsert(
            table_path, _strip_extra_columns(rows), _mute_decisions_column_types_base()
        )
    logging.info(f"Wrote {len(rows)} mute decisions to {table_path}")
    return len(rows)


def write_pattern_matches(
    ydb_wrapper: YDBWrapper,
    branch: str,
    build_type: str,
    matches: List[Dict[str, Any]],
) -> int:
    """Write all pattern matches (alert, log) to mute_decisions."""
    if not matches:
        return 0

    table_path = ydb_wrapper.get_table_path("mute_decisions")
    create_mute_decisions_table(ydb_wrapper, table_path)

    now = datetime.datetime.now(datetime.timezone.utc)
    rows = []

    for m in matches:
        full_name = m.get("full_name")
        if not full_name and m.get("suite_folder"):
            full_name = m["suite_folder"]
        if not full_name:
            full_name = m.get("suite_folder", "") + "/" + m.get("test_name", "")
        rule_id = m.get("rule_id", "")
        reaction = m.get("reaction", "log")
        action = f"{reaction}:{rule_id}" if rule_id else reaction

        match_details = {k: v for k, v in m.items() if k not in ("rule_id", "reaction", "full_name")}
        match_details["pattern"] = m.get("pattern", "")
        match_details["rule_id"] = rule_id

        rows.append({
            "timestamp": now,
            "full_name": full_name,
            "build_type": build_type,
            "branch": branch,
            "action": action,
            "rule_id": rule_id,
            "reason": json.dumps(match_details, default=str)[:4096] if match_details else "",
            "previous_state": None,
            "new_state": None,
            "match_details": match_details,
            "behavior_start_date": m.get("behavior_start_date"),
            "behavior_start_commit": m.get("behavior_start_commit"),
            "behavior_start_pr": m.get("behavior_start_pr"),
        })

    try:
        ydb_wrapper.bulk_upsert(table_path, rows, _mute_decisions_column_types())
    except Exception as e:
        logging.warning(f"bulk_upsert with extra columns failed: {e}. Retrying with base schema.")
        ydb_wrapper.bulk_upsert(
            table_path, _strip_extra_columns(rows), _mute_decisions_column_types_base()
        )
    logging.info(f"Wrote {len(rows)} pattern matches to {table_path}")
    return len(rows)
