#!/usr/bin/env python3

MANUAL_FAST_UNMUTE_STATUS = "manual_fast_unmute"
IDLE_STATUS = "idle"
RESOLVED_RULE_MANUAL = "manual"
RESOLVED_RULE_DEFAULT = "default"
STATUS_SOURCE_CONTROL_COMMENT = "control_comment"
STATUS_SOURCE_LEGACY = "legacy"

# Canonical schema for test_results/analytics/mute_control_state.
# Tuple format: (column_name, ydb_primitive_type, nullable)
MANUAL_UNMUTE_TABLE_SCHEMA = [
    ("issue_number", "Uint64", False),
    ("full_name", "Utf8", False),
    ("branch", "Utf8", False),
    ("build_type", "Utf8", False),
    ("issue_state", "Utf8", True),
    ("issue_closed_by_login", "Utf8", True),
    ("issue_closed_by_type", "Utf8", True),
    ("linked_pr_count", "Uint32", True),
    ("manual_unmute_status", "Utf8", True),
    ("manual_request_active", "Uint8", True),
    ("manual_requested_at", "Timestamp", True),
    ("resolved_at", "Timestamp", True),
    ("resolved_rule", "Utf8", True),
    ("resolved_window_days", "Uint32", True),
    ("last_status_change_at", "Timestamp", True),
    ("status_source", "Utf8", True),
    ("effective_unmute_window_days", "Uint32", True),
    ("default_unmute_window_days", "Uint32", True),
    ("manual_fast_unmute_window_days", "Uint32", True),
    ("resolution_reason", "Utf8", True),
    ("exported_at", "Timestamp", False),
]

MANUAL_UNMUTE_TABLE_PRIMARY_KEY = ("branch", "build_type", "full_name", "issue_number")
MANUAL_UNMUTE_TABLE_PARTITION_BY_HASH = ("branch",)
MANUAL_UNMUTE_TABLE_STORE = "COLUMN"


def render_manual_unmute_create_table_sql(table_path):
    column_lines = []
    for name, type_name, nullable in MANUAL_UNMUTE_TABLE_SCHEMA:
        suffix = "" if nullable else " NOT NULL"
        column_lines.append(f"`{name}` {type_name}{suffix}")

    primary_key = ", ".join(f"`{name}`" for name in MANUAL_UNMUTE_TABLE_PRIMARY_KEY)
    partition_keys = ", ".join(f"`{name}`" for name in MANUAL_UNMUTE_TABLE_PARTITION_BY_HASH)

    return (
        f"CREATE TABLE IF NOT EXISTS `{table_path}` (\n"
        f"    {',\n    '.join(column_lines)},\n"
        f"    PRIMARY KEY ({primary_key})\n"
        f")\n"
        f"PARTITION BY HASH({partition_keys})\n"
        f"WITH (\n"
        f"    STORE = {MANUAL_UNMUTE_TABLE_STORE}\n"
        f")"
    )

def normalize_manual_unmute_status(status, requested=False):
    if not status:
        return MANUAL_FAST_UNMUTE_STATUS if requested else IDLE_STATUS
    return status


def build_manual_unmute_row_payload(
    status,
    state,
    requested,
    resolution_reason,
    manual_requested_at=None,
    resolved_at=None,
    resolved_rule=None,
    resolved_window_days=None,
    last_status_change_at=None,
    status_source=None,
):
    normalized_status = normalize_manual_unmute_status(status, requested=requested)
    is_active_requested = int(state == "active" and bool(requested))
    return {
        "manual_unmute_status": normalized_status,
        "manual_request_active": is_active_requested,
        "manual_requested_at": manual_requested_at,
        "resolved_at": resolved_at,
        "resolved_rule": resolved_rule,
        "resolved_window_days": resolved_window_days,
        "last_status_change_at": last_status_change_at,
        "status_source": status_source,
        "resolution_reason": resolution_reason,
    }
