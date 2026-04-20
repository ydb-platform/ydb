#!/usr/bin/env python3

"""Manual fast-unmute state machine.

When a user manually closes a mute issue as Completed, every test listed in
that issue that is currently still muted gets a row in the YDB table
``fast_unmute_active``. While a row exists, `create_new_muted_ya.py` evaluates
the test against a shorter unmute window (see `mute_config.json`) instead of
the default one. Rows are cleaned up when the test is either unmuted, fails
during the fast window, or the window expires.

Usage:
    python3 manual_unmute.py sync
    python3 manual_unmute.py sync -v   # DEBUG: why each issue/test was skipped
"""

import argparse
import datetime
import logging
import os
import sys

import ydb

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(_SCRIPT_DIR, '..', 'analytics'))
sys.path.append(_SCRIPT_DIR)
sys.path.append(os.path.join(_SCRIPT_DIR, '..'))

from github_issue_utils import DEFAULT_BUILD_TYPE, parse_body
from update_mute_issues import (
    ORG_NAME,
    REPO_NAME,
    add_issue_comment,
    run_query,
)
from ydb_wrapper import YDBWrapper

from mute_constants import (
    get_manual_unmute_currently_muted_lookback_days,
    get_manual_unmute_issue_closed_lookback_days,
    get_manual_unmute_min_runs,
    get_manual_unmute_window_days,
    get_mute_window_days,
)

LABEL_NAME = 'manual-fast-unmute'

_LABEL_ID_CACHE = {}

# GitHub ``__typename`` is ``User`` for PAT-based bot accounts; skip known bot logins (M2).
BOT_LOGINS = frozenset({'ydbot', 'github-actions'})

# Verbose (`sync -v`) touches only this logger so ydb/grpc stay at INFO (no RPC spam).
_LOG = logging.getLogger('manual_unmute')


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
    """Fast-track window/min-runs — same keys as ``mute_constants`` / ``mute_config.json``."""
    return {
        'window_days': get_manual_unmute_window_days(),
        'min_runs': get_manual_unmute_min_runs(),
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


_MONITOR_EPOCH = datetime.date(1970, 1, 1)


def _date_window_to_days(date_window):
    """Normalize ``tests_monitor.date_window`` to days since 1970-01-01 (matches create_new_muted_ya)."""
    if date_window is None:
        return None
    if isinstance(date_window, datetime.datetime):
        date_window = date_window.date()
    if isinstance(date_window, datetime.date):
        return (date_window - _MONITOR_EPOCH).days
    return int(date_window)


def create_manual_unmute_table(ydb_wrapper, table_path):
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS `{table_path}` (
        `full_name`            Utf8      NOT NULL,
        `branch`               Utf8      NOT NULL,
        `build_type`           Utf8      NOT NULL,
        `github_issue_number`  Uint64    NOT NULL,
        `requested_at`         Timestamp NOT NULL,
        `window_days`          Uint32    NOT NULL,
        PRIMARY KEY (full_name, branch, build_type)
    )
    WITH (STORE = COLUMN)
    """
    ydb_wrapper.create_table(table_path, create_sql)


def fetch_all_rows(ydb_wrapper, table_path):
    query = f"""
    SELECT full_name, branch, build_type, github_issue_number, requested_at, window_days
    FROM `{table_path}`
    """
    return ydb_wrapper.execute_scan_query(query, query_name='manual_unmute_fetch_all')


def count_rows_per_issue(ydb_wrapper, table_path, issue_numbers):
    """Return {issue_number: remaining_row_count} for the given issues."""
    numbers = sorted({int(n) for n in (issue_numbers or []) if n is not None})
    if not numbers:
        return {}
    in_list = ','.join(str(n) for n in numbers)
    query = f"""
    SELECT github_issue_number AS n, COUNT(*) AS c
    FROM `{table_path}`
    WHERE github_issue_number IN ({in_list})
    GROUP BY github_issue_number
    """
    rows = ydb_wrapper.execute_scan_query(query, query_name='manual_unmute_count_remaining')
    return {int(r['n']): int(r['c']) for r in rows if r.get('n') is not None}


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
    lb = int(get_manual_unmute_currently_muted_lookback_days())
    br = _escape(branch)
    bt = _escape(build_type)
    query = f"""
    SELECT t.full_name AS full_name
    FROM `{tests_monitor_path}` AS t
    INNER JOIN (
        SELECT full_name AS fn, MAX(date_window) AS max_date_window
        FROM `{tests_monitor_path}`
        WHERE branch = '{br}'
            AND build_type = '{bt}'
            AND date_window >= CurrentUtcDate() - {lb} * Interval("P1D")
        GROUP BY full_name
    ) AS last_row
        ON t.full_name = last_row.fn AND t.date_window = last_row.max_date_window
    WHERE t.branch = '{br}'
        AND t.build_type = '{bt}'
        AND t.is_muted = 1
    """
    rows = ydb_wrapper.execute_scan_query(query, query_name='manual_unmute_currently_muted')
    return {row['full_name'] for row in rows if row.get('full_name')}


def fetch_monitor_outcomes_in_days(ydb_wrapper, tests_monitor_path, branch, build_type, lookback_days):
    """Daily rows from ``tests_monitor`` for one branch/build (bounded scan).

    In CI, a failed run that is muted is stored as status ``mute`` → ``mute_count``;
    non-muted failures are ``fail_count`` (see ``transform_build_results.mute_test_result`` /
    ``upload_tests_results``). For fast-unmute revert we follow the same ``total_fails`` idea
    as ``create_new_muted_ya`` (``fail_count`` + ``mute_count``), then anchor per-row by
    ``requested_at`` in cleanup so pre-entry history does not spuriously revert (C1 review).
    """
    query = f"""
    SELECT full_name, date_window, fail_count, mute_count
    FROM `{tests_monitor_path}`
    WHERE branch = '{_escape(branch)}'
        AND build_type = '{_escape(build_type)}'
        AND date_window >= CurrentUtcDate() - {int(lookback_days)} * Interval("P1D")
    """
    return ydb_wrapper.execute_scan_query(query, query_name='manual_unmute_outcomes_window')


def bad_outcomes_in_day_range(monitor_rows, full_name, since_days_inclusive):
    """Sum ``fail_count`` + ``mute_count`` for ``full_name`` on monitor days ``>= since_days_inclusive``.

    Used for rollback: same trailing calendar span as ``aggregate_test_data(..., period_days)``
    in ``create_new_muted_ya`` (see ``start_date = today - (period_days - 1)``), bounded below
    by fast-unmute entry so pre-entry history never counts (C1).
    """
    if since_days_inclusive is None:
        return 0
    total = 0
    for row in monitor_rows:
        if row.get('full_name') != full_name:
            continue
        dw = _date_window_to_days(row.get('date_window'))
        if dw is None or dw < since_days_inclusive:
            continue
        total += int(row.get('fail_count') or 0) + int(row.get('mute_count') or 0)
    return total


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


def _get_label_id():
    """Resolve the pre-created label node id (cached). Returns None if missing."""
    if LABEL_NAME in _LABEL_ID_CACHE:
        return _LABEL_ID_CACHE[LABEL_NAME]

    query = """
    query ($owner: String!, $name: String!, $labelName: String!) {
      repository(owner: $owner, name: $name) {
        label(name: $labelName) { id }
      }
    }
    """
    result = run_query(
        query,
        {'owner': ORG_NAME, 'name': REPO_NAME, 'labelName': LABEL_NAME},
    )
    label = (((result.get('data') or {}).get('repository') or {}).get('label') or {})
    label_id = label.get('id')
    if not label_id:
        logging.warning(
            "Label %r not found in %s/%s — create it manually in the repository labels page",
            LABEL_NAME, ORG_NAME, REPO_NAME,
        )
        return None
    _LABEL_ID_CACHE[LABEL_NAME] = label_id
    return label_id


def add_label_to_issue(issue_id):
    """Attach the fast-unmute label. Idempotent — GitHub ignores duplicates."""
    label_id = _get_label_id()
    if not label_id:
        return
    mutation = """
    mutation ($labelableId: ID!, $labelIds: [ID!]!) {
      addLabelsToLabelable(input: {labelableId: $labelableId, labelIds: $labelIds}) {
        labelable { __typename }
      }
    }
    """
    try:
        run_query(mutation, {'labelableId': issue_id, 'labelIds': [label_id]})
    except Exception as exc:
        logging.warning('Failed to add label to issue %s: %s', issue_id, exc)


def remove_label_from_issue(issue_id):
    """Detach the fast-unmute label. No-op if label is not present."""
    label_id = _get_label_id()
    if not label_id:
        return
    mutation = """
    mutation ($labelableId: ID!, $labelIds: [ID!]!) {
      removeLabelsFromLabelable(input: {labelableId: $labelableId, labelIds: $labelIds}) {
        labelable { __typename }
      }
    }
    """
    try:
        run_query(mutation, {'labelableId': issue_id, 'labelIds': [label_id]})
    except Exception as exc:
        logging.warning('Failed to remove label from issue %s: %s', issue_id, exc)


def create_fast_unmute_grace_table(ydb_wrapper, table_path):
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS `{table_path}` (
        `full_name`                   Utf8      NOT NULL,
        `branch`                      Utf8      NOT NULL,
        `build_type`                  Utf8      NOT NULL,
        `github_issue_number`         Uint64    NOT NULL,
        `fast_track_requested_at`     Timestamp NOT NULL,
        `grace_started_at`            Timestamp NOT NULL,
        PRIMARY KEY (full_name, branch, build_type)
    )
    WITH (STORE = COLUMN)
    """
    ydb_wrapper.create_table(table_path, create_sql)


def upsert_fast_unmute_grace_row(
    ydb_wrapper,
    table_path,
    full_name,
    branch,
    build_type,
    github_issue_number,
    fast_track_requested_at,
    grace_started_at,
):
    column_types = (
        ydb.BulkUpsertColumns()
        .add_column('full_name', ydb.PrimitiveType.Utf8)
        .add_column('branch', ydb.PrimitiveType.Utf8)
        .add_column('build_type', ydb.PrimitiveType.Utf8)
        .add_column('github_issue_number', ydb.PrimitiveType.Uint64)
        .add_column('fast_track_requested_at', ydb.PrimitiveType.Timestamp)
        .add_column('grace_started_at', ydb.PrimitiveType.Timestamp)
    )
    rows = [
        {
            'full_name': full_name,
            'branch': branch,
            'build_type': build_type,
            'github_issue_number': int(github_issue_number),
            'fast_track_requested_at': fast_track_requested_at,
            'grace_started_at': grace_started_at,
        }
    ]
    ydb_wrapper.bulk_upsert(table_path, rows, column_types)


def expire_fast_unmute_grace(ydb_wrapper, table_path, manual_window_days, mute_window_days):
    """Remove grace rows once the ladder has reached the full mute window (``mute_window_days``)."""
    query = f"""
    SELECT full_name, branch, build_type, grace_started_at
    FROM `{table_path}`
    """
    try:
        rows = ydb_wrapper.execute_scan_query(query, query_name='fast_unmute_grace_expire_scan')
    except Exception as exc:
        logging.warning('expire_fast_unmute_grace: scan failed: %s', exc)
        return

    today = datetime.datetime.now(tz=datetime.timezone.utc).date()
    threshold = mute_window_days - manual_window_days
    if threshold < 1:
        threshold = 1

    for row in rows:
        gs = _coerce_dt(row.get('grace_started_at'))
        if not gs:
            continue
        gs_date = gs.astimezone(datetime.timezone.utc).date()
        if (today - gs_date).days >= threshold:
            delete_grace_row(
                ydb_wrapper,
                table_path,
                row.get('full_name'),
                row.get('branch'),
                row.get('build_type'),
            )


def delete_grace_row(ydb_wrapper, table_path, full_name, branch, build_type):
    if not full_name or not branch or not build_type:
        return
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
        query_name='fast_unmute_grace_delete',
    )


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
        .add_column('window_days', ydb.PrimitiveType.Uint32)
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

    raw_candidates = fetch_candidate_issues(
        ydb_wrapper, issues_table_path, get_manual_unmute_issue_closed_lookback_days()
    )
    # One issue can appear twice if linked from multiple projects (M4).
    candidates = list(
        {int(c['issue_number']): c for c in raw_candidates if c.get('issue_number') is not None}.values()
    )
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
            _LOG.debug(
                'enter: skip candidate without issue_number/issue_id: number=%r id=%r',
                issue_number_raw,
                issue_id,
            )
            continue
        issue_number = int(issue_number_raw)

        closer = closers.get(issue_number) or {}
        if closer.get('type') != 'User':
            _LOG.debug(
                'enter: skip #%s: closer is not User (login=%r type=%r)',
                issue_number,
                closer.get('login'),
                closer.get('type'),
            )
            continue
        login = (closer.get('login') or '').lower()
        if login in BOT_LOGINS:
            _LOG.debug(
                'enter: skip #%s: closer login %r is bot-denylisted',
                issue_number,
                login,
            )
            continue

        parsed = parse_body(issue.get('body') or '')
        tests = parsed.tests
        branches = parsed.branches or ['main']
        build_type = parsed.build_type or DEFAULT_BUILD_TYPE
        if not tests:
            _LOG.debug('enter: skip #%s: no tests parsed from issue body', issue_number)
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
                    _LOG.debug(
                        'enter: skip #%s test %r: not currently muted on branch=%r build_type=%r',
                        issue_number,
                        full_name,
                        branch,
                        build_type,
                    )
                    continue
                row_key = (full_name, branch, build_type)
                if row_key in existing:
                    _LOG.debug(
                        'enter: skip #%s test %r: row already in fast_unmute_active %s',
                        issue_number,
                        full_name,
                        row_key,
                    )
                    continue
                issue_rows.append({
                    'full_name': full_name,
                    'branch': branch,
                    'build_type': build_type,
                    'github_issue_number': issue_number,
                    'requested_at': now,
                    'window_days': window_days,
                })

        if not issue_rows:
            _LOG.debug(
                'enter: skip #%s: zero rows after filtering (parsed tests=%s branches=%s build_type=%s)',
                issue_number,
                sorted(tests),
                branches,
                build_type,
            )
            continue

        upsert_rows(ydb_wrapper, table_path, issue_rows)

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
        add_label_to_issue(issue_id)

        new_rows.extend(issue_rows)
        for row in issue_rows:
            existing[(row['full_name'], row['branch'], row['build_type'])] = row

    logging.info('manual_unmute_enter: inserted %d row(s) from %d candidate issue(s)',
                 len(new_rows), len(candidates))


def cleanup_manual_unmute(ydb_wrapper, table_path, tests_monitor_path, default_window_days):
    """Drop rows that have served their purpose, failed, or expired.

    TTL is computed from the ``window_days`` stored on each row (the value
    captured at enter time). ``default_window_days`` is used only as a fallback
    for rows without a stored value (e.g. legacy rows).
    """
    rows = fetch_all_rows(ydb_wrapper, table_path)
    if not rows:
        return

    now = datetime.datetime.now(tz=datetime.timezone.utc)
    run_url = workflow_run_url()

    grouped = {}
    for row in rows:
        key = (row.get('branch'), row.get('build_type'))
        if not key[0] or not key[1]:
            continue
        grouped.setdefault(key, []).append(row)

    fails_by_issue = {}
    affected_issues = set()
    delete_count = 0
    grace_table_path = None
    try:
        grace_table_path = ydb_wrapper.get_table_path('fast_unmute_grace')
        create_fast_unmute_grace_table(ydb_wrapper, grace_table_path)
    except KeyError:
        pass

    for (branch, build_type), group_rows in grouped.items():
        currently_muted = fetch_currently_muted(ydb_wrapper, tests_monitor_path, branch, build_type)
        # Failure lookback must cover the longest row TTL (2 × window_days per row).
        check_window = max(
            (2 * int(r.get('window_days') or default_window_days) for r in group_rows),
            default=2 * default_window_days,
        )
        monitor_rows = fetch_monitor_outcomes_in_days(
            ydb_wrapper, tests_monitor_path, branch, build_type, check_window
        )
        for row in group_rows:
            full_name = row.get('full_name')
            if not full_name:
                continue
            requested_at = _coerce_dt(row.get('requested_at'))
            issue_number = row.get('github_issue_number')
            row_window = int(row.get('window_days') or default_window_days)
            ttl = datetime.timedelta(days=row_window * 2)

            # Rollback uses the same *rolling* calendar window width as manual unmute in
            # create_new_muted_ya (``manual_unmute_window_days`` == row_window), not a cumulative
            # sum since entry. Stale reds from before the fix age out of the trailing window; we
            # still ignore any day strictly before fast-unmute entry (max with requested_at date).
            today = now.date()
            trailing_start = today - datetime.timedelta(days=row_window - 1)
            if requested_at:
                entry_date = requested_at.astimezone(datetime.timezone.utc).date()
                effective_start = max(trailing_start, entry_date)
            else:
                effective_start = trailing_start
            since_days = (effective_start - _MONITOR_EPOCH).days

            if full_name not in currently_muted:
                if grace_table_path:
                    try:
                        ft_at = requested_at or now
                        upsert_fast_unmute_grace_row(
                            ydb_wrapper,
                            grace_table_path,
                            full_name,
                            branch,
                            build_type,
                            int(issue_number or 0),
                            ft_at,
                            now,
                        )
                    except Exception as exc:
                        logging.warning('Failed to record fast-unmute grace for %s: %s', full_name, exc)
                delete_row(ydb_wrapper, table_path, full_name, branch, build_type)
                delete_count += 1
                if issue_number:
                    affected_issues.add(int(issue_number))
                logging.info('manual_unmute_cleanup: %s (already unmuted)', full_name)
                continue

            if bad_outcomes_in_day_range(monitor_rows, full_name, since_days) > 0:
                delete_row(ydb_wrapper, table_path, full_name, branch, build_type)
                delete_count += 1
                if issue_number:
                    affected_issues.add(int(issue_number))
                    fails_by_issue.setdefault(int(issue_number), []).append(full_name)
                logging.info('manual_unmute_cleanup: %s (failed during window)', full_name)
                continue

            if requested_at and (now - requested_at) > ttl:
                delete_row(ydb_wrapper, table_path, full_name, branch, build_type)
                delete_count += 1
                if issue_number:
                    affected_issues.add(int(issue_number))
                logging.info('manual_unmute_cleanup: %s (window expired)', full_name)

    if affected_issues:
        remaining = count_rows_per_issue(ydb_wrapper, table_path, affected_issues)
        issues_to_delabel = {num for num in affected_issues if remaining.get(num, 0) == 0}
        issue_ids = _fetch_issue_node_ids(affected_issues)

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

        for issue_number in issues_to_delabel:
            issue_id = issue_ids.get(issue_number)
            if issue_id:
                remove_label_from_issue(issue_id)

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

    table_path = ydb_wrapper.get_table_path('fast_unmute_active')
    issues_table_path = ydb_wrapper.get_table_path('issues')
    tests_monitor_path = ydb_wrapper.get_table_path('tests_monitor')

    create_manual_unmute_table(ydb_wrapper, table_path)
    try:
        grace_table_path = ydb_wrapper.get_table_path('fast_unmute_grace')
        create_fast_unmute_grace_table(ydb_wrapper, grace_table_path)
    except KeyError:
        grace_table_path = None

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
    if grace_table_path:
        expire_fast_unmute_grace(
            ydb_wrapper,
            grace_table_path,
            config['window_days'],
            get_mute_window_days(),
        )


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

    if not os.environ.get('GITHUB_TOKEN'):
        logging.error(
            'GITHUB_TOKEN is required for GitHub GraphQL (issue close, labels, timeline). '
            'Set it in workflow env or export it when running locally.'
        )
        return 1

    parser = argparse.ArgumentParser(description='Manual fast-unmute state machine')
    subparsers = parser.add_subparsers(dest='mode', required=True)
    sync_parser = subparsers.add_parser(
        'sync',
        help='Enter new rows and clean up stale/failed/unmuted rows',
    )
    sync_parser.add_argument(
        '-v',
        '--verbose',
        action='store_true',
        help='Log enter-phase skip reasons (does not enable ydb/grpc DEBUG)',
    )
    args = parser.parse_args()
    if getattr(args, 'verbose', False):
        _LOG.setLevel(logging.DEBUG)

    with YDBWrapper() as ydb_wrapper:
        if not ydb_wrapper.check_credentials():
            return 1
        sync(ydb_wrapper)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
