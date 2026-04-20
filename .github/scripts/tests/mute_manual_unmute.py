#!/usr/bin/env python3

"""Manual fast-unmute state machine.

When a user manually closes a mute issue as Completed, every test listed in
that issue that is currently still muted gets a row in the YDB table
`mute_manual_unmute`. While a row exists, `create_new_muted_ya.py` evaluates
the test against a shorter unmute window (see `mute_config.json`) instead of
the default one. Rows are cleaned up when the test is either unmuted, fails
during the fast window, or the window expires.

Usage:
    python3 mute_manual_unmute.py sync
"""

import argparse
import datetime
import json
import logging
import os
import sys

import ydb

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'analytics'))
from ydb_wrapper import YDBWrapper

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from update_mute_issues import (
    ORG_NAME,
    REPO_NAME,
    add_issue_comment,
    run_query,
)

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from github_issue_utils import DEFAULT_BUILD_TYPE, parse_body


CONFIG_PATH = os.path.normpath(
    os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'mute_config.json')
)

# Lookback for CLOSED+COMPLETED issues when picking new fast-unmute candidates.
# Issues closed earlier than this will not be reopened by this script even if
# they still have muted tests — this keeps the scan cheap and bounds surprises.
ISSUE_CLOSED_LOOKBACK_DAYS = 14


COMMENT_ENTER = """🚀 **Fast-unmute started**

User **{closer_login}** closed this issue, signalling that the listed tests are fixed.
The following tests are now on a **shorter unmute window** ({window_days} days, min {min_runs} clean runs) instead of the default:

{tests_bullet_list}

**What happens next**

- If the tests stay green within the window → they are unmuted automatically and this issue is closed again.
- If any of these tests fails during the window → the test is removed from fast-unmute and returns to the default criteria. This issue stays open.
- No action needed from you. Please do not edit `muted_ya.txt` manually.

🔗 Workflow run: {workflow_run_url}
"""


COMMENT_FAIL = """⚠️ **Fast-unmute reverted**

The following tests failed during the fast-unmute window and are back on the default unmute criteria:

{tests_bullet_list}

🔗 Workflow run: {workflow_run_url}
"""


def load_config():
    with open(CONFIG_PATH, 'r', encoding='utf-8') as fp:
        payload = json.load(fp)
    for key in ('manual_unmute_window_days', 'manual_unmute_min_runs'):
        if key not in payload:
            raise RuntimeError(f"Missing key '{key}' in {CONFIG_PATH}")
        if int(payload[key]) <= 0:
            raise ValueError(f"'{key}' in {CONFIG_PATH} must be positive")
    return {
        'window_days': int(payload['manual_unmute_window_days']),
        'min_runs': int(payload['manual_unmute_min_runs']),
    }


def workflow_run_url():
    server = os.environ.get('GITHUB_SERVER_URL', 'https://github.com')
    repo = os.environ.get('GITHUB_REPOSITORY', '')
    run_id = os.environ.get('GITHUB_RUN_ID', '')
    if repo and run_id:
        return f"{server}/{repo}/actions/runs/{run_id}"
    return 'N/A'


def _escape(value):
    return str(value).replace("'", "''")


def _coerce_dt(value):
    if value is None:
        return None
    if isinstance(value, datetime.datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=datetime.timezone.utc)
        return value.astimezone(datetime.timezone.utc)
    if isinstance(value, int):
        return datetime.datetime.fromtimestamp(value / 1_000_000, tz=datetime.timezone.utc)
    return None


def create_manual_unmute_table(ydb_wrapper, table_path):
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS `{table_path}` (
        `full_name`            Utf8      NOT NULL,
        `branch`               Utf8      NOT NULL,
        `build_type`           Utf8      NOT NULL,
        `github_issue_number`  Uint64    NOT NULL,
        `requested_at`         Timestamp NOT NULL,
        PRIMARY KEY (full_name, branch, build_type)
    )
    WITH (STORE = COLUMN)
    """
    ydb_wrapper.create_table(table_path, create_sql)


def fetch_all_rows(ydb_wrapper, table_path):
    query = f"""
    SELECT full_name, branch, build_type, github_issue_number, requested_at
    FROM `{table_path}`
    """
    return ydb_wrapper.execute_scan_query(query, query_name='manual_unmute_fetch_all')


def fetch_candidate_issues(ydb_wrapper, issues_table_path, lookback_days):
    query = f"""
    SELECT issue_id, issue_number, body
    FROM `{issues_table_path}`
    WHERE state = 'CLOSED'
        AND state_reason = 'COMPLETED'
        AND closed_at >= CurrentUtcTimestamp() - {int(lookback_days)} * Interval("P1D")
    """
    return ydb_wrapper.execute_scan_query(query, query_name='manual_unmute_candidate_issues')


def fetch_currently_muted(ydb_wrapper, tests_monitor_path, branch, build_type):
    query = f"""
    SELECT DISTINCT full_name
    FROM `{tests_monitor_path}`
    WHERE branch = '{_escape(branch)}'
        AND build_type = '{_escape(build_type)}'
        AND is_muted = 1
        AND date_window >= CurrentUtcDate() - 1 * Interval("P1D")
    """
    rows = ydb_wrapper.execute_scan_query(query, query_name='manual_unmute_currently_muted')
    return {row['full_name'] for row in rows if row.get('full_name')}


def fetch_failures_since(ydb_wrapper, tests_monitor_path, branch, build_type, full_names, window_days):
    """Return {full_name: total_fail_count} over the last `window_days` for the given test set."""
    if not full_names:
        return {}
    fails = {name: 0 for name in full_names}
    query = f"""
    SELECT full_name, fail_count, mute_count
    FROM `{tests_monitor_path}`
    WHERE branch = '{_escape(branch)}'
        AND build_type = '{_escape(build_type)}'
        AND date_window >= CurrentUtcDate() - {int(window_days)} * Interval("P1D")
    """
    rows = ydb_wrapper.execute_scan_query(query, query_name='manual_unmute_failures')
    for row in rows:
        name = row.get('full_name')
        if name not in fails:
            continue
        fails[name] += int(row.get('fail_count') or 0) + int(row.get('mute_count') or 0)
    return fails


def fetch_issue_closers(issue_numbers):
    """Return {issue_number: {'login': str, 'type': 'User'|'Bot'|''}}.

    We need this because the exported `issues` table does not carry "closed by",
    so for the short list of candidates we query GitHub directly.
    """
    result = {}
    numbers = sorted({int(n) for n in (issue_numbers or []) if n is not None})
    if not numbers:
        return result
    chunk_size = 50
    for i in range(0, len(numbers), chunk_size):
        chunk = numbers[i:i + chunk_size]
        subqueries = []
        for number in chunk:
            subqueries.append(
                f"""
                n{number}: issue(number: {number}) {{
                    timelineItems(last: 20, itemTypes: [CLOSED_EVENT]) {{
                        nodes {{
                            ... on ClosedEvent {{
                                actor {{ __typename login }}
                            }}
                        }}
                    }}
                }}
                """
            )
        query = f"""
        query {{
            repository(owner: "{ORG_NAME}", name: "{REPO_NAME}") {{
                {' '.join(subqueries)}
            }}
        }}
        """
        response = run_query(query)
        repo_data = (response.get('data') or {}).get('repository') or {}
        for number in chunk:
            node = repo_data.get(f'n{number}')
            login = ''
            actor_type = ''
            if node:
                events = (node.get('timelineItems') or {}).get('nodes') or []
                for event in reversed(events):
                    actor = event.get('actor') or {}
                    login = actor.get('login') or ''
                    actor_type = actor.get('__typename') or ''
                    if login:
                        break
            result[number] = {'login': login, 'type': actor_type}
    return result


def reopen_issue(issue_id):
    """Reopen a closed issue. Safe to call on already-open issues (no-op)."""
    state_query = """
    query ($issueId: ID!) {
      node(id: $issueId) {
        ... on Issue { state }
      }
    }
    """
    state_result = run_query(state_query, {'issueId': issue_id})
    state = ((state_result.get('data') or {}).get('node') or {}).get('state')
    if state != 'CLOSED':
        return
    mutation = """
    mutation ($issueId: ID!) {
      reopenIssue(input: {issueId: $issueId}) { issue { id } }
    }
    """
    run_query(mutation, {'issueId': issue_id})


def upsert_rows(ydb_wrapper, table_path, rows):
    if not rows:
        return
    column_types = (
        ydb.BulkUpsertColumns()
        .add_column('full_name', ydb.PrimitiveType.Utf8)
        .add_column('branch', ydb.PrimitiveType.Utf8)
        .add_column('build_type', ydb.PrimitiveType.Utf8)
        .add_column('github_issue_number', ydb.PrimitiveType.Uint64)
        .add_column('requested_at', ydb.PrimitiveType.Timestamp)
    )
    ydb_wrapper.bulk_upsert(table_path, rows, column_types)


def delete_row(ydb_wrapper, table_path, full_name, branch, build_type):
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
        {'$full_name': full_name, '$branch': branch, '$build_type': build_type},
        query_name='manual_unmute_delete_row',
    )


def _format_bullet_list(tests):
    return '\n'.join(f"- `{name}`" for name in sorted(set(tests)))


def enter_manual_unmute(ydb_wrapper, table_path, issues_table_path, tests_monitor_path, window_days, min_runs):
    """Discover newly-closed-by-human issues and register their still-muted tests."""
    existing = {
        (r['full_name'], r['branch'], r['build_type']): r
        for r in fetch_all_rows(ydb_wrapper, table_path)
        if r.get('full_name') and r.get('branch') and r.get('build_type')
    }

    candidates = fetch_candidate_issues(ydb_wrapper, issues_table_path, ISSUE_CLOSED_LOOKBACK_DAYS)
    if not candidates:
        logging.info('manual_unmute_enter: no CLOSED+COMPLETED candidates in lookback window')
        return

    issue_numbers = {int(c['issue_number']) for c in candidates if c.get('issue_number') is not None}
    closers = fetch_issue_closers(issue_numbers)

    muted_cache = {}
    now = datetime.datetime.now(tz=datetime.timezone.utc)
    run_url = workflow_run_url()
    new_rows = []

    for issue in candidates:
        issue_number_raw = issue.get('issue_number')
        issue_id = issue.get('issue_id')
        if issue_number_raw is None or not issue_id:
            continue
        issue_number = int(issue_number_raw)

        closer = closers.get(issue_number) or {}
        if closer.get('type') != 'User':
            continue

        parsed = parse_body(issue.get('body') or '')
        tests = parsed.tests
        branches = parsed.branches or ['main']
        build_type = parsed.build_type or DEFAULT_BUILD_TYPE
        if not tests:
            continue

        issue_rows = []
        for full_name in tests:
            for branch in branches:
                cache_key = (branch, build_type)
                if cache_key not in muted_cache:
                    muted_cache[cache_key] = fetch_currently_muted(
                        ydb_wrapper, tests_monitor_path, branch, build_type
                    )
                if full_name not in muted_cache[cache_key]:
                    continue
                row_key = (full_name, branch, build_type)
                if row_key in existing:
                    continue
                issue_rows.append({
                    'full_name': full_name,
                    'branch': branch,
                    'build_type': build_type,
                    'github_issue_number': issue_number,
                    'requested_at': now,
                })

        if not issue_rows:
            continue

        reopen_issue(issue_id)
        add_issue_comment(
            issue_id,
            COMMENT_ENTER.format(
                closer_login=closer.get('login') or 'unknown',
                window_days=window_days,
                min_runs=min_runs,
                tests_bullet_list=_format_bullet_list(r['full_name'] for r in issue_rows),
                workflow_run_url=run_url,
            ),
        )

        new_rows.extend(issue_rows)
        for row in issue_rows:
            existing[(row['full_name'], row['branch'], row['build_type'])] = row

    upsert_rows(ydb_wrapper, table_path, new_rows)
    logging.info('manual_unmute_enter: inserted %d row(s) from %d candidate issue(s)',
                 len(new_rows), len(candidates))


def cleanup_manual_unmute(ydb_wrapper, table_path, tests_monitor_path, window_days):
    """Drop rows that have served their purpose, failed, or expired."""
    rows = fetch_all_rows(ydb_wrapper, table_path)
    if not rows:
        return

    now = datetime.datetime.now(tz=datetime.timezone.utc)
    ttl = datetime.timedelta(days=window_days * 2)
    run_url = workflow_run_url()

    grouped = {}
    for row in rows:
        key = (row.get('branch'), row.get('build_type'))
        if not key[0] or not key[1]:
            continue
        grouped.setdefault(key, []).append(row)

    fails_by_issue = {}
    delete_count = 0
    for (branch, build_type), group_rows in grouped.items():
        currently_muted = fetch_currently_muted(ydb_wrapper, tests_monitor_path, branch, build_type)
        full_names = {r.get('full_name') for r in group_rows if r.get('full_name')}
        failures = fetch_failures_since(
            ydb_wrapper, tests_monitor_path, branch, build_type, full_names, window_days
        )

        for row in group_rows:
            full_name = row.get('full_name')
            if not full_name:
                continue
            requested_at = _coerce_dt(row.get('requested_at'))
            issue_number = row.get('github_issue_number')

            if full_name not in currently_muted:
                delete_row(ydb_wrapper, table_path, full_name, branch, build_type)
                delete_count += 1
                logging.info('manual_unmute_cleanup: %s (already unmuted)', full_name)
                continue

            if failures.get(full_name, 0) > 0:
                delete_row(ydb_wrapper, table_path, full_name, branch, build_type)
                delete_count += 1
                logging.info('manual_unmute_cleanup: %s (failed during window)', full_name)
                if issue_number:
                    fails_by_issue.setdefault(int(issue_number), []).append(full_name)
                continue

            if requested_at and (now - requested_at) > ttl:
                delete_row(ydb_wrapper, table_path, full_name, branch, build_type)
                delete_count += 1
                logging.info('manual_unmute_cleanup: %s (window expired)', full_name)

    if fails_by_issue:
        issue_ids = _fetch_issue_node_ids(fails_by_issue.keys())
        for issue_number, failed_tests in fails_by_issue.items():
            issue_id = issue_ids.get(issue_number)
            if not issue_id:
                continue
            add_issue_comment(
                issue_id,
                COMMENT_FAIL.format(
                    tests_bullet_list=_format_bullet_list(failed_tests),
                    workflow_run_url=run_url,
                ),
            )

    logging.info('manual_unmute_cleanup: removed %d row(s)', delete_count)


def _fetch_issue_node_ids(issue_numbers):
    """Return {issue_number: issue_node_id}."""
    result = {}
    numbers = sorted({int(n) for n in issue_numbers or []})
    if not numbers:
        return result
    chunk_size = 50
    for i in range(0, len(numbers), chunk_size):
        chunk = numbers[i:i + chunk_size]
        subqueries = [f"n{n}: issue(number: {n}) {{ id }}" for n in chunk]
        query = f"""
        query {{
            repository(owner: "{ORG_NAME}", name: "{REPO_NAME}") {{
                {' '.join(subqueries)}
            }}
        }}
        """
        response = run_query(query)
        repo_data = (response.get('data') or {}).get('repository') or {}
        for number in chunk:
            node = repo_data.get(f'n{number}')
            if node and node.get('id'):
                result[number] = node['id']
    return result


def sync(ydb_wrapper):
    config = load_config()

    table_path = ydb_wrapper.get_table_path('mute_manual_unmute')
    issues_table_path = ydb_wrapper.get_table_path('issues')
    tests_monitor_path = ydb_wrapper.get_table_path('tests_monitor')

    create_manual_unmute_table(ydb_wrapper, table_path)
    enter_manual_unmute(
        ydb_wrapper,
        table_path,
        issues_table_path,
        tests_monitor_path,
        config['window_days'],
        config['min_runs'],
    )
    cleanup_manual_unmute(
        ydb_wrapper,
        table_path,
        tests_monitor_path,
        config['window_days'],
    )


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

    parser = argparse.ArgumentParser(description='Manual fast-unmute state machine')
    subparsers = parser.add_subparsers(dest='mode', required=True)
    subparsers.add_parser('sync', help='Enter new rows and clean up stale/failed/unmuted rows')
    parser.parse_args()

    with YDBWrapper() as ydb_wrapper:
        if not ydb_wrapper.check_credentials():
            return 1
        sync(ydb_wrapper)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
