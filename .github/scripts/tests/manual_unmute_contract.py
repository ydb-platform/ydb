#!/usr/bin/env python3

MANUAL_FAST_UNMUTE_STATUS = "manual_fast_unmute"
IDLE_STATUS = "idle"

# Canonical schema for test_results/analytics/manual_unmute_requests.
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
    ("hours_until_ready", "Uint32", True),
    ("manual_wait_hours", "Uint32", True),
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
    requested_at,
    resolution_reason,
    wait_hours,
    wait_hours_left,
):
    normalized_status = normalize_manual_unmute_status(status, requested=requested)
    is_active_requested = int(state == "active" and bool(requested))
    wait_hours_total = int(wait_hours)
    wait_hours_left_total = int(wait_hours_left)
    return {
        "manual_unmute_status": normalized_status,
        "manual_request_active": is_active_requested,
        "manual_requested_at": requested_at,
        "hours_until_ready": wait_hours_left_total,
        "manual_wait_hours": wait_hours_total,
        "resolution_reason": resolution_reason,
    }
