#!/usr/bin/env python3
"""
Mute decisions history — хранит причины mute/unmute/delete/graduation для трассировки.

Использование:
  - create_new_muted_ya.py вызывает write_mute_decisions() после apply_and_add_mutes
  - Данные пишутся в test_results/analytics/mute_decisions
"""

import datetime
import logging
from typing import List, Optional, Set

import ydb

from ydb_wrapper import YDBWrapper


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
) -> int:
    """
    Write mute decisions to YDB for traceability.

    Args:
        ydb_wrapper: YDB connection
        branch, build_type: context
        to_mute, to_unmute, to_delete: lists of "suite test_name" strings
        to_graduated: set of "suite test_name" (quarantine graduation)
        *_rule_id: rule IDs from pattern_rules.yaml
        *_debug: optional parallel lists of reason strings (same order as to_*)

    Returns:
        Number of rows written.
    """
    to_mute_debug_map = dict(zip(to_mute, to_mute_debug)) if to_mute_debug and len(to_mute_debug) == len(to_mute) else {}
    to_unmute_debug_map = dict(zip(to_unmute, to_unmute_debug)) if to_unmute_debug and len(to_unmute_debug) == len(to_unmute) else {}
    to_delete_debug_map = dict(zip(to_delete, to_delete_debug)) if to_delete_debug and len(to_delete_debug) == len(to_delete) else {}

    table_path = ydb_wrapper.get_table_path("mute_decisions")
    create_mute_decisions_table(ydb_wrapper, table_path)

    now = datetime.datetime.now(datetime.timezone.utc)
    rows = []

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
        })

    if not rows:
        logging.info("No mute decisions to write")
        return 0

    column_types = ydb.BulkUpsertColumns()
    column_types.add_column("timestamp", ydb.PrimitiveType.Timestamp)
    column_types.add_column("full_name", ydb.PrimitiveType.Utf8)
    column_types.add_column("build_type", ydb.PrimitiveType.Utf8)
    column_types.add_column("branch", ydb.PrimitiveType.Utf8)
    column_types.add_column("action", ydb.PrimitiveType.Utf8)
    column_types.add_column("rule_id", ydb.OptionalType(ydb.PrimitiveType.Utf8))
    column_types.add_column("reason", ydb.OptionalType(ydb.PrimitiveType.Utf8))
    column_types.add_column("previous_state", ydb.OptionalType(ydb.PrimitiveType.Utf8))
    column_types.add_column("new_state", ydb.OptionalType(ydb.PrimitiveType.Utf8))

    ydb_wrapper.bulk_upsert(table_path, rows, column_types)
    logging.info(f"Wrote {len(rows)} mute decisions to {table_path}")
    return len(rows)
