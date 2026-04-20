#!/usr/bin/env python3

import argparse
import datetime
import json
import os
import sys

import ydb

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'analytics'))
from ydb_wrapper import YDBWrapper

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from update_mute_issues import (
    add_issue_comment,
    get_project_v2_fields,
    run_query,
    update_issue_status,
    ORG_NAME,
    PROJECT_ID,
    REPO_NAME,
)
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from github_issue_utils import DEFAULT_BUILD_TYPE, parse_body


CONFIG_PATH = os.path.normpath(
    os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'mute_observation_config.json')
)

STATUS_MUTED = "Muted"
STATUS_OBSERVATION = "Observation"

COMMENT_OBSERVATION_ENTERED = """🔒 **Observation started**

User **{closed_by_login}** closed this issue, marking the tests as fixed.
The tests have been moved to observation automatically.

**What happens next:**
- Tests are hidden from the mute dashboard immediately
- Tests remain muted in CI for up to {observation_window_days} days
- If stable → they will be unmuted automatically, this issue will be closed
- If any test fails again → issue will return to **Muted** status

**No action needed from you.** Do not edit `muted_ya.txt` manually.

⏱ Observation until: {observation_until_date}
🔗 Workflow run: {workflow_run_url}
"""

COMMENT_OBSERVATION_FAILED = """⚠️ **Returned to Muted**

Test **{failed_test_name}** failed during the observation period — the fix did not hold.

**Details:**
- Failed test: {failed_test_name}
- Failures detected: {fail_count}
- Observation started: {observation_since}

**What to do:**
1. Investigate the failures
2. Apply a fix
3. Close this issue again when ready

🔗 Workflow run: {workflow_run_url}
"""

COMMENT_OBSERVATION_EXPIRED = """✅ **Observation complete**

Test **{full_name}** was stable for {observation_window_days} days. Observation period is over.
The test will be unmuted automatically in the next automation run.

This issue will be closed shortly.

🔗 Workflow run: {workflow_run_url}
"""


def load_observation_window_days() -> int:
    with open(CONFIG_PATH, 'r', encoding='utf-8') as config_file:
        config = json.load(config_file)
    days = int(config["observation_window_days"])
    if days <= 0:
        raise ValueError("observation_window_days must be a positive integer")
    return days


def workflow_run_url() -> str:
    server = os.environ.get("GITHUB_SERVER_URL", "https://github.com")
    repository = os.environ.get("GITHUB_REPOSITORY", "")
    run_id = os.environ.get("GITHUB_RUN_ID", "")
    if repository and run_id:
        return f"{server}/{repository}/actions/runs/{run_id}"
    return "N/A"


def _timestamp_to_datetime(value):
    if value is None:
        return None
    if isinstance(value, datetime.datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=datetime.timezone.utc)
        return value.astimezone(datetime.timezone.utc)
    if isinstance(value, int):
        return datetime.datetime.fromtimestamp(value / 1_000_000, tz=datetime.timezone.utc)
    return None


def _escape_yql_string(value) -> str:
    return str(value).replace("'", "''")


def get_status_option_ids():
    _, project_fields = get_project_v2_fields(ORG_NAME, PROJECT_ID)
    status_field_id = None
    status_options = {}
    for field in project_fields:
        if field.get("name", "").lower() != "status":
            continue
        status_field_id = field["id"]
        for option in field.get("options", []):
            option_name = option.get("name")
            if option_name:
                status_options[option_name] = option["id"]
        break
    if not status_field_id:
        raise RuntimeError("Status field was not found in project fields")
    if STATUS_MUTED not in status_options or STATUS_OBSERVATION not in status_options:
        raise RuntimeError("Required status options are missing in project fields")
    return status_field_id, status_options


def create_observation_table(ydb_wrapper, table_path):
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS `{table_path}` (
        `full_name`             Utf8      NOT NULL,
        `branch`                Utf8      NOT NULL,
        `build_type`            Utf8      NOT NULL,
        `github_issue_number`   Uint64    NOT NULL,
        `github_issue_id`       Utf8,
        `observation_since`      Timestamp NOT NULL,
        PRIMARY KEY (full_name, branch, build_type)
    )
    WITH (STORE = COLUMN)
    """
    ydb_wrapper.create_table(table_path, create_sql)


def fetch_observation_rows(ydb_wrapper, table_path):
    query = f"""
    SELECT
        full_name,
        branch,
        build_type,
        github_issue_number,
        github_issue_id,
        observation_since
    FROM `{table_path}`
    """
    return ydb_wrapper.execute_scan_query(query, query_name="mute_observation_fetch_rows")


def fetch_candidate_closed_issues(ydb_wrapper, issues_table_path):
    query = f"""
    SELECT
        issue_id,
        issue_number,
        body,
        state,
        state_reason
    FROM `{issues_table_path}`
    WHERE state = 'CLOSED'
        AND state_reason = 'COMPLETED'
    """
    return ydb_wrapper.execute_scan_query(query, query_name="mute_observation_closed_issues")


def fetch_currently_muted_tests(ydb_wrapper, tests_monitor_table, branch, build_type):
    escaped_branch = _escape_yql_string(branch)
    escaped_build_type = _escape_yql_string(build_type)
    query = f"""
    SELECT DISTINCT
        full_name
    FROM `{tests_monitor_table}`
    WHERE branch = '{escaped_branch}'
        AND build_type = '{escaped_build_type}'
        AND is_muted = 1
        AND date_window >= CurrentUtcDate() - 1 * Interval("P1D")
    """
    rows = ydb_wrapper.execute_scan_query(
        query,
        query_name="mute_observation_currently_muted_tests",
    )
    return {row.get("full_name") for row in rows if row.get("full_name")}


def fetch_project_issue_refs(issue_numbers=None):
    refs = {}
    required_numbers = set(issue_numbers or [])
    has_next_page = True
    end_cursor = "null"
    while has_next_page:
        query = """
        {
          organization(login: "%s") {
            projectV2(number: %s) {
              items(first: 100, after: %s) {
                nodes {
                  id
                  content {
                    ... on Issue {
                      id
                      number
                      url
                    }
                  }
                }
                pageInfo {
                  hasNextPage
                  endCursor
                }
              }
            }
          }
        }
        """ % (ORG_NAME, PROJECT_ID, end_cursor)
        result = run_query(query)
        items = result.get("data", {}).get("organization", {}).get("projectV2", {}).get("items", {})
        for item in items.get("nodes", []):
            content = item.get("content") or {}
            issue_number = content.get("number")
            if issue_number is None:
                continue
            issue_number = int(issue_number)
            if required_numbers and issue_number not in required_numbers:
                continue
            refs[issue_number] = {
                "project_item_id": item.get("id"),
                "issue_id": content.get("id"),
                "issue_url": content.get("url"),
            }
        if required_numbers and len(refs) >= len(required_numbers):
            break
        page_info = items.get("pageInfo") or {}
        has_next_page = bool(page_info.get("hasNextPage"))
        end_cursor = f"\"{page_info['endCursor']}\"" if page_info.get("endCursor") else "null"
    return refs


def fetch_issue_closer_types_and_logins(issue_numbers):
    closers = {}
    if not issue_numbers:
        return closers

    numbers = sorted(issue_numbers)
    chunk_size = 50
    for i in range(0, len(numbers), chunk_size):
        chunk = numbers[i : i + chunk_size]
        issue_nodes_query = []
        for number in chunk:
            issue_nodes_query.append(
                f"""
                n{number}: issue(number: {number}) {{
                    number
                    timelineItems(last: 20, itemTypes: [CLOSED_EVENT]) {{
                        nodes {{
                            __typename
                            ... on ClosedEvent {{
                                actor {{
                                    __typename
                                    login
                                }}
                                closer {{
                                    __typename
                                }}
                            }}
                        }}
                    }}
                }}
                """
            )
        query = f"""
        query {{
            repository(owner: "{ORG_NAME}", name: "{REPO_NAME}") {{
                {' '.join(issue_nodes_query)}
            }}
        }}
        """
        result = run_query(query)
        repo_data = result.get("data", {}).get("repository", {})
        for number in chunk:
            node = repo_data.get(f"n{number}")
            if not node:
                continue
            actor_login = None
            actor_type = None
            timeline_nodes = node.get("timelineItems", {}).get("nodes", [])
            for event in reversed(timeline_nodes):
                if event.get("__typename") != "ClosedEvent":
                    continue
                actor = event.get("actor") or {}
                actor_login = actor.get("login")
                actor_type = actor.get("__typename")
                break
            closers[number] = {
                "closed_by_type": "User" if actor_type == "User" else "Bot",
                "closed_by_login": actor_login or "",
            }
    return closers


def reopen_issue(issue_id):
    state_query = """
    query ($issueId: ID!) {
      node(id: $issueId) {
        ... on Issue {
          state
        }
      }
    }
    """
    state_result = run_query(state_query, {"issueId": issue_id})
    issue_state = (
        state_result.get("data", {})
        .get("node", {})
        .get("state")
    )
    if issue_state != "CLOSED":
        return

    query = """
    mutation ($issueId: ID!) {
      reopenIssue(input: {issueId: $issueId}) {
        issue {
          id
          url
        }
      }
    }
    """
    run_query(query, {"issueId": issue_id})


def upsert_observation_rows(ydb_wrapper, table_path, rows):
    if not rows:
        return
    column_types = (
        ydb.BulkUpsertColumns()
        .add_column("full_name", ydb.PrimitiveType.Utf8)
        .add_column("branch", ydb.PrimitiveType.Utf8)
        .add_column("build_type", ydb.PrimitiveType.Utf8)
        .add_column("github_issue_number", ydb.PrimitiveType.Uint64)
        .add_column("github_issue_id", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("observation_since", ydb.PrimitiveType.Timestamp)
    )
    ydb_wrapper.bulk_upsert(table_path, rows, column_types)


def delete_observation_row(ydb_wrapper, table_path, full_name, branch, build_type):
    query = f"""
    DECLARE $full_name AS Utf8;
    DECLARE $branch AS Utf8;
    DECLARE $build_type AS Utf8;

    DELETE FROM `{table_path}`
    WHERE full_name = $full_name
        AND branch = $branch
        AND build_type = $build_type;
    """
    ydb_wrapper.execute_dml(
        query,
        {
            "$full_name": full_name,
            "$branch": branch,
            "$build_type": build_type,
        },
        query_name="mute_observation_delete_row",
    )


def process_enter_observation(
    ydb_wrapper,
    table_path,
    issues_table_path,
    tests_monitor_table,
    observation_window_days,
    status_field_id,
    status_options,
):
    existing_rows = fetch_observation_rows(ydb_wrapper, table_path)
    existing_keys = {
        (
            row.get("full_name"),
            row.get("branch"),
            row.get("build_type"),
        )
        for row in existing_rows
        if row.get("full_name") and row.get("branch") and row.get("build_type")
    }

    closed_issues = fetch_candidate_closed_issues(ydb_wrapper, issues_table_path)
    candidate_numbers = {
        int(row["issue_number"])
        for row in closed_issues
        if row.get("issue_number") is not None
    }
    closer_info = fetch_issue_closer_types_and_logins(candidate_numbers)
    issue_refs = fetch_project_issue_refs(candidate_numbers)
    run_url = workflow_run_url()
    now = datetime.datetime.now(tz=datetime.timezone.utc)
    observation_until = now + datetime.timedelta(days=observation_window_days)
    currently_muted_cache = {}

    new_rows = []
    for issue in closed_issues:
        issue_number_raw = issue.get("issue_number")
        body = issue.get("body", "")
        if issue_number_raw is None:
            continue
        issue_number = int(issue_number_raw)
        issue_ref = issue_refs.get(issue_number) or {}
        issue_id = issue_ref.get("issue_id") or issue.get("issue_id")
        project_item_id = issue_ref.get("project_item_id")
        issue_url = issue_ref.get("issue_url") or f"https://github.com/{ORG_NAME}/{REPO_NAME}/issues/{issue_number}"
        if not issue_id or not project_item_id:
            continue
        closer = closer_info.get(issue_number, {})
        if closer.get("closed_by_type") != "User":
            continue

        parsed = parse_body(body or "")
        tests = parsed.tests
        branches = parsed.branches or ["main"]
        build_type = parsed.build_type or DEFAULT_BUILD_TYPE
        if not tests:
            continue

        issue_rows = []
        for full_name in tests:
            for branch in branches:
                muted_cache_key = (branch, build_type)
                if muted_cache_key not in currently_muted_cache:
                    currently_muted_cache[muted_cache_key] = fetch_currently_muted_tests(
                        ydb_wrapper,
                        tests_monitor_table,
                        branch,
                        build_type,
                    )
                if full_name not in currently_muted_cache[muted_cache_key]:
                    continue
                row_key = (full_name, branch, build_type)
                if row_key in existing_keys:
                    continue
                issue_rows.append(
                    {
                        "full_name": full_name,
                        "branch": branch,
                        "build_type": build_type,
                        "github_issue_number": issue_number,
                        "github_issue_id": issue_id,
                        "observation_since": now,
                    }
                )
        if not issue_rows:
            continue

        reopen_issue(issue_id)
        update_issue_status(
            project_item_id,
            status_field_id,
            status_options[STATUS_OBSERVATION],
            issue_url,
        )
        comment = COMMENT_OBSERVATION_ENTERED.format(
            closed_by_login=closer.get("closed_by_login") or "unknown",
            observation_window_days=observation_window_days,
            observation_until_date=observation_until.strftime("%Y-%m-%d"),
            workflow_run_url=run_url,
        )
        add_issue_comment(issue_id, comment)
        new_rows.extend(issue_rows)
        for issue_row in issue_rows:
            existing_keys.add((issue_row["full_name"], issue_row["branch"], issue_row["build_type"]))
    deduped_rows = {}
    for row in new_rows:
        row_key = (row["full_name"], row["branch"], row["build_type"])
        deduped_rows[row_key] = row
    upsert_observation_rows(ydb_wrapper, table_path, list(deduped_rows.values()))


def _scan_date_to_date(value):
    if value is None:
        return None
    if isinstance(value, datetime.datetime):
        return value.date()
    if isinstance(value, datetime.date):
        return value
    if isinstance(value, int):
        if value < 100_000:
            return datetime.date(1970, 1, 1) + datetime.timedelta(days=value)
        if value < 10_000_000_000:
            return datetime.datetime.fromtimestamp(value, tz=datetime.timezone.utc).date()
        return datetime.datetime.fromtimestamp(value / 1_000_000, tz=datetime.timezone.utc).date()
    return None


def fetch_monitor_failures_bulk(ydb_wrapper, tests_monitor_table, observation_rows, days):
    failures_by_key = {}
    grouped = {}
    for row in observation_rows:
        full_name = row.get("full_name")
        branch = row.get("branch")
        build_type = row.get("build_type")
        if not full_name or not branch or not build_type:
            continue
        key = (branch, build_type)
        if key not in grouped:
            grouped[key] = {"full_names": set(), "since": {}}
        grouped[key]["full_names"].add(full_name)
        observation_since = _timestamp_to_datetime(row.get("observation_since"))
        grouped[key]["since"][full_name] = observation_since.date() if observation_since else None
        failures_by_key[(full_name, branch, build_type)] = 0

    for (branch, build_type), group_data in grouped.items():
        escaped_branch = _escape_yql_string(branch)
        escaped_build_type = _escape_yql_string(build_type)
        query = f"""
        SELECT
            full_name,
            fail_count,
            date_window
        FROM `{tests_monitor_table}`
        WHERE branch = '{escaped_branch}'
            AND build_type = '{escaped_build_type}'
            AND date_window >= CurrentUtcDate() - {days} * Interval("P1D")
        """
        monitor_rows = ydb_wrapper.execute_scan_query(
            query,
            query_name="mute_observation_monitor_failures_bulk",
        )
        for monitor_row in monitor_rows:
            full_name = monitor_row.get("full_name")
            if full_name not in group_data["full_names"]:
                continue
            row_date = _scan_date_to_date(monitor_row.get("date_window"))
            observation_since_date = group_data["since"].get(full_name)
            if observation_since_date and row_date and row_date < observation_since_date:
                continue
            fail_count = int(monitor_row.get("fail_count") or 0)
            if fail_count <= 0:
                continue
            failures_by_key[(full_name, branch, build_type)] += fail_count
    return failures_by_key


def process_failed_observation(
    ydb_wrapper,
    table_path,
    tests_monitor_table,
    observation_window_days,
    status_field_id,
    status_options,
):
    rows = fetch_observation_rows(ydb_wrapper, table_path)
    if not rows:
        return
    run_url = workflow_run_url()
    issue_numbers = {
        int(row["github_issue_number"])
        for row in rows
        if row.get("github_issue_number") is not None
    }
    issue_refs = fetch_project_issue_refs(issue_numbers)
    failures_by_key = fetch_monitor_failures_bulk(
        ydb_wrapper,
        tests_monitor_table,
        rows,
        observation_window_days,
    )
    rows_by_issue = {}
    failed_rows_by_issue = {}
    for row in rows:
        full_name = row.get("full_name")
        branch = row.get("branch")
        build_type = row.get("build_type")
        issue_number_raw = row.get("github_issue_number")
        if not full_name or not branch or not build_type or issue_number_raw is None:
            continue
        issue_number = int(issue_number_raw)
        rows_by_issue.setdefault(issue_number, []).append(row)
        fail_count = failures_by_key.get((full_name, branch, build_type), 0)
        if fail_count > 0:
            failed_rows_by_issue.setdefault(issue_number, []).append((row, fail_count))

    for issue_number, failed_rows in failed_rows_by_issue.items():
        issue_ref = issue_refs.get(issue_number) or {}
        fallback_issue_id = failed_rows[0][0].get("github_issue_id")
        issue_id = issue_ref.get("issue_id") or fallback_issue_id
        if not issue_id:
            continue
        project_item_id = issue_ref.get("project_item_id")
        issue_url = issue_ref.get("issue_url") or f"https://github.com/{ORG_NAME}/{REPO_NAME}/issues/{issue_number}"

        for issue_row in rows_by_issue.get(issue_number, []):
            delete_observation_row(
                ydb_wrapper,
                table_path,
                issue_row.get("full_name"),
                issue_row.get("branch"),
                issue_row.get("build_type"),
            )
        if project_item_id:
            update_issue_status(
                project_item_id,
                status_field_id,
                status_options[STATUS_MUTED],
                issue_url,
            )

        for failed_row, fail_count in failed_rows:
            full_name = failed_row.get("full_name")
            observation_since = _timestamp_to_datetime(failed_row.get("observation_since"))
            comment = COMMENT_OBSERVATION_FAILED.format(
                failed_test_name=full_name,
                fail_count=fail_count,
                observation_since=observation_since.strftime("%Y-%m-%d %H:%M:%S UTC")
                if observation_since
                else "unknown",
                workflow_run_url=run_url,
            )
            add_issue_comment(issue_id, comment)


def process_expired_observation(ydb_wrapper, table_path, observation_window_days):
    rows = fetch_observation_rows(ydb_wrapper, table_path)
    now = datetime.datetime.now(tz=datetime.timezone.utc)
    run_url = workflow_run_url()
    issue_numbers = {
        int(row["github_issue_number"])
        for row in rows
        if row.get("github_issue_number") is not None
    }
    issue_refs = fetch_project_issue_refs(issue_numbers)
    for row in rows:
        full_name = row.get("full_name")
        branch = row.get("branch")
        build_type = row.get("build_type")
        issue_number_raw = row.get("github_issue_number")
        issue_number = int(issue_number_raw) if issue_number_raw is not None else None
        observation_since = _timestamp_to_datetime(row.get("observation_since"))
        if not full_name or not branch or not build_type or not observation_since:
            continue
        if observation_since > now - datetime.timedelta(days=observation_window_days):
            continue

        delete_observation_row(ydb_wrapper, table_path, full_name, branch, build_type)
        issue_ref = issue_refs.get(issue_number) if issue_number is not None else {}
        issue_id = (issue_ref or {}).get("issue_id") or row.get("github_issue_id")
        if issue_id:
            comment = COMMENT_OBSERVATION_EXPIRED.format(
                full_name=full_name,
                observation_window_days=observation_window_days,
                workflow_run_url=run_url,
            )
            add_issue_comment(issue_id, comment)


def main():
    parser = argparse.ArgumentParser(description="Mute observation state machine")
    parser.parse_args()

    observation_window_days = load_observation_window_days()

    with YDBWrapper() as ydb_wrapper:
        if not ydb_wrapper.check_credentials():
            return 1

        observation_table = ydb_wrapper.get_table_path("mute_observation")
        issues_table = ydb_wrapper.get_table_path("issues")
        tests_monitor_table = ydb_wrapper.get_table_path("tests_monitor")

        create_observation_table(ydb_wrapper, observation_table)
        status_field_id, status_options = get_status_option_ids()

        process_enter_observation(
            ydb_wrapper,
            observation_table,
            issues_table,
            tests_monitor_table,
            observation_window_days,
            status_field_id,
            status_options,
        )
        process_failed_observation(
            ydb_wrapper,
            observation_table,
            tests_monitor_table,
            observation_window_days,
            status_field_id,
            status_options,
        )
        process_expired_observation(ydb_wrapper, observation_table, observation_window_days)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
