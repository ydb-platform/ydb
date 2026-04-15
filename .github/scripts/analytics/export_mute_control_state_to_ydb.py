#!/usr/bin/env python3

"""
Export mute control state from GitHub issues to YDB.
"""

import datetime
import os
import sys
import time

import ydb

sys.path.insert(0, os.path.normpath(os.path.join(os.path.dirname(__file__), "..")))
from github_issue_utils import parse_body

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "tests"))
from update_mute_issues import (
    ORG_NAME,
    PROJECT_ID,
    _parse_control_items,
    fetch_all_issues,
)
from mute_thresholds import get_thresholds
from manual_unmute_contract import (
    MANUAL_UNMUTE_TABLE_SCHEMA,
    RESOLVED_RULE_DEFAULT,
    RESOLVED_RULE_MANUAL,
    STATUS_SOURCE_CONTROL_COMMENT,
    STATUS_SOURCE_LEGACY,
    build_manual_unmute_row_payload,
    render_manual_unmute_create_table_sql,
)
from export_issues_to_ydb import fetch_all_issue_comment_nodes
from ydb_wrapper import YDBWrapper


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
    s = str(ts).strip()
    normalized = s.replace("Z", "+00:00")
    try:
        dt = datetime.datetime.fromisoformat(normalized)
    except ValueError:
        dt = None
    if dt is None and "T" in s and len(s) >= 19:
        try:
            dt = datetime.datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S")
        except ValueError:
            dt = None
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    return dt.astimezone(datetime.timezone.utc)


def _is_updated_within_last_day(issue, now):
    updated_at = _to_dt((issue or {}).get("updatedAt"))
    if updated_at is None:
        return False
    return updated_at >= (now - datetime.timedelta(days=1))


def _resolve_rule_for_item(state, reason):
    if state != "resolved":
        return None
    if (reason or "").strip() == "stable_manual_fast_window":
        return RESOLVED_RULE_MANUAL
    return RESOLVED_RULE_DEFAULT


def _days_since(ts, now):
    dt = _to_dt(ts)
    if dt is None:
        return 0
    delta = now.date() - dt.date()
    return max(0, int(delta.days))


def _effective_unmute_window(
    *,
    requested,
    state,
    resolved_rule,
    resolved_at,
    default_window_days,
    fast_window_days,
    now,
):
    is_manual_active = bool(requested) and (state == "active")
    if is_manual_active:
        return int(fast_window_days)
    if resolved_rule == RESOLVED_RULE_MANUAL:
        days_since_resolved = _days_since(resolved_at, now)
        return int(min(int(default_window_days), int(fast_window_days) + days_since_resolved))
    return int(default_window_days)


def _is_open_issue(issue):
    return ((issue or {}).get("state") or "").upper() == "OPEN"


def _has_existing_rows(ydb_wrapper, table_path):
    try:
        result = ydb_wrapper.execute_scan_query(
            f"SELECT MAX(exported_at) AS max_exported_at FROM `{table_path}`"
        )
    except Exception as exc:
        print(f"Warning: failed to check existing rows in {table_path}: {exc}")
        return False
    if not result:
        return False
    return bool(result[0].get("max_exported_at"))


def collect_rows(default_window_days, fast_window_days, incremental_only):
    """Build export rows from GitHub (issues in org mute Project V2 only)."""
    stage_started_at = time.time()
    project_nodes = fetch_all_issues(ORG_NAME, PROJECT_ID)
    print(
        f"Fetched {len(project_nodes)} project items from mute project {PROJECT_ID} "
        f"({ORG_NAME})"
    )
    now = datetime.datetime.now(datetime.timezone.utc)
    candidate_issues = []
    for node in project_nodes:
        issue = (node or {}).get("content") or {}
        if not issue.get("id") or issue.get("number") is None:
            continue
        if not incremental_only and not _is_open_issue(issue):
            continue
        if not incremental_only or _is_updated_within_last_day(issue, now):
            candidate_issues.append(issue)
    if incremental_only:
        print(f"Project issues updated in last 24h: {len(candidate_issues)}")
    else:
        print(
            "No existing mute_control_state rows found: "
            f"using full project scan for OPEN issues only ({len(candidate_issues)} issues)"
        )
    print(
        "Selection stage done in "
        f"{time.time() - stage_started_at:.2f}s"
    )

    rows = []
    issue_processing_started_at = time.time()
    mute_candidate_issues = 0
    comments_fetch_count = 0
    total_comments_loaded = 0
    total_tests_processed = 0
    total_issue_rows = 0
    total_candidates = len(candidate_issues)

    for idx, issue in enumerate(candidate_issues, start=1):
        issue_number = issue.get("number")
        if idx == 1 or idx % 25 == 0 or idx == total_candidates:
            print(
                f"[issue {idx}/{total_candidates}] "
                f"#{issue_number}: evaluating mute markers"
            )
        body = issue.get("body", "") or ""
        if "<!--mute_list_start-->" not in body or "<!--mute_list_end-->" not in body:
            continue
        mute_candidate_issues += 1

        if not issue_number:
            continue

        issue_id = issue.get("id")
        if not issue_id:
            continue

        parsed = parse_body(body)
        branches = parsed.branches
        build_type = parsed.build_type
        tests_from_body = set(parsed.tests)

        issue_state = issue.get("state", "")
        comments_fetch_count += 1
        comments_nodes = fetch_all_issue_comment_nodes(issue_id)
        total_comments_loaded += len(comments_nodes)
        print(
            f"[issue {idx}/{total_candidates}] "
            f"#{issue_number}: comments fetched={len(comments_nodes)}"
        )
        close_actor_login, close_actor_type, linked_pr_numbers = _parse_issue_timeline(issue)
        if issue_state != "CLOSED":
            close_actor_login = ""
            close_actor_type = ""

        control_items = {}
        for node in comments_nodes:
            control_items.update(_parse_control_items((node or {}).get("body", "")))

        all_tests = sorted(tests_from_body | set(control_items))
        total_tests_processed += len(all_tests)
        issue_rows_before = len(rows)
        for full_name in all_tests:
            has_control_item = full_name in control_items
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
            state = item.get("state", "active")
            requested = bool(item.get("requested"))
            reason = item.get("reason", "") or None
            manual_requested_at = _to_dt(item.get("requested_at"))
            resolved_at = _to_dt(item.get("resolved_at"))
            resolved_rule = _resolve_rule_for_item(state, reason)
            resolved_window_days = (
                int(fast_window_days) if resolved_rule == RESOLVED_RULE_MANUAL else int(default_window_days)
            ) if resolved_rule else None
            last_status_change_at = resolved_at or manual_requested_at or now
            status_source = STATUS_SOURCE_CONTROL_COMMENT if has_control_item else STATUS_SOURCE_LEGACY

            for branch in branches:
                row = {
                    "issue_number": int(issue_number),
                    "full_name": full_name,
                    "branch": branch,
                    "build_type": build_type,
                    "issue_state": issue_state,
                    "issue_closed_by_login": close_actor_login,
                    "issue_closed_by_type": close_actor_type,
                    "linked_pr_count": len(linked_pr_numbers),
                    "effective_unmute_window_days": _effective_unmute_window(
                        requested=requested,
                        state=state,
                        resolved_rule=resolved_rule,
                        resolved_at=resolved_at,
                        default_window_days=default_window_days,
                        fast_window_days=fast_window_days,
                        now=now,
                    ),
                    "default_unmute_window_days": int(default_window_days),
                    "manual_fast_unmute_window_days": int(fast_window_days),
                    "exported_at": now,
                }

                row.update(
                    build_manual_unmute_row_payload(
                        status=item.get("status", "idle"),
                        state=state,
                        requested=requested,
                        resolution_reason=reason,
                        manual_requested_at=manual_requested_at,
                        resolved_at=resolved_at,
                        resolved_rule=resolved_rule,
                        resolved_window_days=resolved_window_days,
                        last_status_change_at=last_status_change_at,
                        status_source=status_source,
                    )
                )

                rows.append(
                    row
                )
        issue_rows_added = len(rows) - issue_rows_before
        total_issue_rows += issue_rows_added
        print(
            f"[issue {idx}/{total_candidates}] "
            f"#{issue_number}: tests={len(all_tests)}, rows_added={issue_rows_added}"
        )
    print(
        "Processing summary: "
        f"candidates={total_candidates}, "
        f"with_mute_markers={mute_candidate_issues}, "
        f"comment_fetches={comments_fetch_count}, "
        f"comments_loaded={total_comments_loaded}, "
        f"tests_processed={total_tests_processed}, "
        f"rows_built={total_issue_rows}, "
        f"duration={time.time() - issue_processing_started_at:.2f}s"
    )
    return rows


def create_table(ydb_wrapper, table_path):
    sql = render_manual_unmute_create_table_sql(table_path)
    ydb_wrapper.create_table(table_path, sql)


def build_column_types():
    columns = ydb.BulkUpsertColumns()

    for name, type_name, nullable in MANUAL_UNMUTE_TABLE_SCHEMA:
        primitive = getattr(ydb.PrimitiveType, type_name)
        ydb_type = ydb.OptionalType(primitive) if nullable else primitive
        columns = columns.add_column(name, ydb_type)

    return columns


def load_thresholds():
    data = get_thresholds()
    fast_window_days = int(data["mute_manual_unmute_window_days"])
    return (
        int(data["mute_default_unmute_window_days"]),
        fast_window_days,
    )


def main():
    start = time.time()
    default_window_days, fast_window_days = load_thresholds()
    print(
        "Thresholds:"
        f" default_unmute_window_days={default_window_days},"
        f" manual_fast_unmute_window_days={fast_window_days}"
    )

    with YDBWrapper() as ydb_wrapper:
        if not ydb_wrapper.check_credentials():
            return 1
        table_path = ydb_wrapper.get_table_path("mute_control_state")
        create_table(ydb_wrapper, table_path)
        incremental_only = _has_existing_rows(ydb_wrapper, table_path)

        rows = collect_rows(
            default_window_days,
            fast_window_days,
            incremental_only=incremental_only,
        )
        print(f"Collected {len(rows)} mute control rows")
        if rows:
            ydb_wrapper.bulk_upsert_batches(
                table_path,
                rows,
                build_column_types(),
                batch_size=500,
                query_name="export_mute_control_state",
            )

    print(f"Done in {time.time() - start:.2f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
