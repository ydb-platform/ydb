#!/usr/bin/env python3

"""
Export manual fast-unmute control state from GitHub issues to YDB.
"""

import datetime
import os
import re
import sys
import time
import ydb

from ydb_wrapper import YDBWrapper
from export_issues_to_ydb import fetch_repository_issues

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "tests"))
from mute_thresholds import get_thresholds
from manual_unmute_contract import (
    MANUAL_UNMUTE_TABLE_COLUMNS,
    build_manual_unmute_row_payload,
    normalize_manual_unmute_status,
)


ORG_NAME = "ydb-platform"
REPO_NAME = "ydb"

MUTE_CONTROL_MARKER = "<!--mute_control_v1-->"
DEFAULT_BRANCH = "main"
DEFAULT_BUILD_TYPE = "relwithdebinfo"


def _parse_body_for_branches(body):
    start = "<!--branch_list_start-->"
    end = "<!--branch_list_end-->"
    if start in body and end in body:
        idx1 = body.find(start) + len(start)
        idx2 = body.find(end, idx1)
        values = [x.strip() for x in body[idx1:idx2].split("\n") if x.strip()]
        if values:
            return values
    return [DEFAULT_BRANCH]


def _parse_issue_tests(body):
    start = "<!--mute_list_start-->"
    end = "<!--mute_list_end-->"
    if start not in body or end not in body:
        return []
    idx1 = body.find(start) + len(start)
    idx2 = body.find(end, idx1)
    return [x.strip() for x in body[idx1:idx2].split("\n") if x.strip().startswith("ydb/")]


def _parse_control_items(comment_body):
    if not comment_body or MUTE_CONTROL_MARKER not in comment_body:
        return {}
    start_idx = comment_body.find(f"{MUTE_CONTROL_MARKER}:start")
    end_idx = comment_body.find(f"{MUTE_CONTROL_MARKER}:end", start_idx if start_idx >= 0 else 0)
    if start_idx < 0 or end_idx < 0:
        return {}

    payload = comment_body[start_idx:end_idx]
    items = {}
    for raw in payload.split("\n"):
        line = raw.strip()
        if not line.startswith("- ["):
            continue
        m = re.search(r"`([^`]+)`", line)
        if not m:
            continue
        test_name = m.group(1).strip()
        requested = line.startswith("- [x]") or line.startswith("- [X]")
        state_match = re.search(r"state:([a-z_]+)", line)
        status_match = re.search(r"status:([a-z0-9_]+)", line)
        reason_match = re.search(r"reason:([a-z0-9_]+)", line)
        requested_at_match = re.search(r"requested_at:([0-9T:\\-+Z]+)", line)
        resolved_at_match = re.search(r"resolved_at:([0-9T:\\-+Z]+)", line)
        status = normalize_manual_unmute_status(
            status_match.group(1) if status_match else "",
            requested=requested,
        )

        items[test_name] = {
            "requested": requested,
            "state": state_match.group(1) if state_match else "active",
            "status": status,
            "reason": reason_match.group(1) if reason_match else "",
            "requested_at": requested_at_match.group(1) if requested_at_match else "",
            "resolved_at": resolved_at_match.group(1) if resolved_at_match else "",
        }
    return items


def _parse_issue_timeline(content):
    timeline_nodes = (content or {}).get("timelineItems", {}).get("nodes", [])
    close_actor_login = ""
    close_actor_type = ""
    for node in reversed(timeline_nodes):
        if node.get("__typename") != "ClosedEvent":
            continue
        actor = node.get("actor") or {}
        close_actor_login = actor.get("login", "") or ""
        close_actor_type = actor.get("__typename", "") or ""
        break

    linked_pr_numbers = set()
    for node in timeline_nodes:
        pr = None
        if node.get("__typename") == "ConnectedEvent":
            subject = node.get("subject") or {}
            if subject.get("__typename") == "PullRequest":
                pr = subject
        elif node.get("__typename") == "CrossReferencedEvent":
            source = node.get("source") or {}
            if source.get("__typename") == "PullRequest":
                pr = source
        if not pr:
            continue
        number = pr.get("number")
        if number is not None:
            linked_pr_numbers.add(int(number))

    return close_actor_login, close_actor_type, linked_pr_numbers


def _to_dt(ts):
    if not ts:
        return None
    try:
        return datetime.datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError:
        return None


def _effective_unmute_window(status, default_window_days, fast_window_days):
    return int(fast_window_days if status == "ready_for_fast_unmute" else default_window_days)


def collect_rows(default_window_days, fast_window_days, wait_hours):
    issues = fetch_repository_issues(ORG_NAME, REPO_NAME, since=None)
    now = datetime.datetime.now(datetime.timezone.utc)
    rows = []

    for issue in issues:
        body = issue.get("body", "") or ""
        if "<!--mute_list_start-->" not in body or "<!--mute_list_end-->" not in body:
            continue

        issue_number = issue.get("number")
        if not issue_number:
            continue

        issue_state = issue.get("state", "")
        branches = _parse_body_for_branches(body)
        tests_from_body = set(_parse_issue_tests(body))
        comments_nodes = ((issue.get("comments") or {}).get("nodes") or [])
        close_actor_login, close_actor_type, linked_pr_numbers = _parse_issue_timeline(issue)

        control_items = {}
        for node in comments_nodes:
            control_items.update(_parse_control_items((node or {}).get("body", "")))

        all_tests = sorted(tests_from_body | set(control_items))
        for full_name in all_tests:
            item = control_items.get(
                full_name,
                {
                    "requested": False,
                    "state": "active",
                    "status": "idle",
                    "reason": "",
                    "requested_at": "",
                    "resolved_at": "",
                },
            )

            requested_at = _to_dt(item.get("requested_at"))
            resolved_at = _to_dt(item.get("resolved_at"))
            wait_hours_left = None
            if requested_at is not None:
                ready_at = requested_at + datetime.timedelta(hours=wait_hours)
                delta = ready_at - now
                wait_hours_left = max(0, int(delta.total_seconds() // 3600))
            manual_wait_hours_left = int(wait_hours_left if wait_hours_left is not None else 0)

            for branch in branches:
                row = {
                    "issue_number": int(issue_number),
                    "full_name": full_name,
                    "branch": branch,
                    "build_type": DEFAULT_BUILD_TYPE,
                    "issue_state": issue_state,
                    "issue_closed_by_login": close_actor_login,
                    "issue_closed_by_type": close_actor_type,
                    "linked_pr_count": len(linked_pr_numbers),
                    "effective_unmute_window_days": _effective_unmute_window(
                        item.get("status", "idle"), default_window_days, fast_window_days
                    ),
                    "default_unmute_window_days": int(default_window_days),
                    "manual_fast_unmute_window_days": int(fast_window_days),
                    "exported_at": now,
                }

                row.update(
                    build_manual_unmute_row_payload(
                        status=item.get("status", "idle"),
                        state=item.get("state", "active"),
                        requested=bool(item.get("requested")),
                        requested_at=requested_at,
                        resolution_reason=item.get("reason", "") or None,
                        wait_hours=int(wait_hours),
                        wait_hours_left=manual_wait_hours_left,
                    )
                )

                rows.append(
                    row
                )
    return rows


def create_table(ydb_wrapper, table_path):
    sql = f"""
        CREATE TABLE IF NOT EXISTS `{table_path}` (
            `issue_number` Uint64 NOT NULL,
            `full_name` Utf8 NOT NULL,
            `branch` Utf8 NOT NULL,
            `build_type` Utf8 NOT NULL,
            `issue_state` Utf8,
            `issue_closed_by_login` Utf8,
            `issue_closed_by_type` Utf8,
            `linked_pr_count` Uint32,
            `manual_unmute_status` Utf8,
            `manual_request_active` Uint8,
            `manual_requested_at` Timestamp,
            `hours_until_ready` Uint32,
            `manual_wait_hours` Uint32,
            `effective_unmute_window_days` Uint32,
            `default_unmute_window_days` Uint32,
            `manual_fast_unmute_window_days` Uint32,
            `resolution_reason` Utf8,
            `exported_at` Timestamp NOT NULL,
            PRIMARY KEY (`branch`, `build_type`, `full_name`, `issue_number`)
        )
        PARTITION BY HASH(`branch`)
        WITH (
            STORE = COLUMN
        )
    """
    ydb_wrapper.create_table(table_path, sql)


def build_column_types():
    columns = ydb.BulkUpsertColumns()

    for name, type_name in MANUAL_UNMUTE_TABLE_COLUMNS:
        primitive = getattr(ydb.PrimitiveType, type_name)
        columns = columns.add_column(name, ydb.OptionalType(primitive))

    return columns


def load_thresholds():
    data = get_thresholds()
    fast_window_days = int(data["manual_fast_unmute_window_days"])
    return (
        int(data["default_unmute_window_days"]),
        fast_window_days,
        fast_window_days * 24,
    )


def main():
    start = time.time()
    default_window_days, fast_window_days, wait_hours = load_thresholds()
    print(
        "Thresholds:"
        f" default_unmute_window_days={default_window_days},"
        f" manual_fast_unmute_window_days={fast_window_days},"
        f" manual_wait_hours={wait_hours} (derived)"
    )

    with YDBWrapper() as ydb_wrapper:
        if not ydb_wrapper.check_credentials():
            return 1
        table_path = ydb_wrapper.get_table_path("manual_unmute_requests")
        create_table(ydb_wrapper, table_path)

        rows = collect_rows(default_window_days, fast_window_days, wait_hours)
        print(f"Collected {len(rows)} manual unmute rows")
        if rows:
            ydb_wrapper.bulk_upsert_batches(
                table_path,
                rows,
                build_column_types(),
                batch_size=500,
                query_name="export_manual_unmute_requests",
            )

    print(f"Done in {time.time() - start:.2f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
