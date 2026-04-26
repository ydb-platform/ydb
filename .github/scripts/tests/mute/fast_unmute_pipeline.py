"""Orchestration: enter / cleanup / sync for manual fast-unmute."""

import datetime
import logging
import os
from collections import defaultdict

from github_issue_utils import DEFAULT_BUILD_TYPE

from mute.constants import (
    get_manual_unmute_issue_closed_lookback_days,
    get_manual_unmute_min_runs,
    get_manual_unmute_ttl_calendar_days,
    get_manual_unmute_window_days,
    get_mute_window_days,
)
from mute.fast_unmute_comments import (
    COMMENT_ABANDON_NOT_COMPLETED,
    COMMENT_ENTER,
    COMMENT_PROGRESS,
    COMMENT_SUCCESS,
    COMMENT_TTL_INCOMPLETE,
    format_bullet_list,
)
from mute.fast_unmute_github import (
    PROJECT_ID,
    PROJECT_STATUS_ON_FAST_UNMUTE_FAIL,
    PROJECT_STATUS_ON_FAST_UNMUTE_REOPEN,
    PROJECT_STATUS_ON_FAST_UNMUTE_SUCCESS,
    add_label_to_issue,
    fetch_issue_closers,
    fetch_issue_label_names,
    fetch_issue_numbers_in_manual_unmute_project,
    fetch_issue_states,
    remove_label_from_issue,
    reopen_issue,
    set_manual_unmute_project_board_status,
)
from mute.fast_unmute_ydb import (
    _coerce_dt,
    count_rows_per_issue,
    create_fast_unmute_grace_table,
    create_manual_unmute_table,
    delete_grace_row,
    delete_row,
    expire_fast_unmute_grace,
    fetch_all_rows,
    fetch_candidate_issues,
    fetch_currently_muted,
    upsert_fast_unmute_grace_row,
    upsert_rows,
)
from mute.update_mute_issues import (
    MANUAL_FAST_UNMUTE_FINISHED_GITHUB_LABEL,
    add_issue_comment,
    parse_body,
)

# GitHub ``__typename`` is ``User`` for PAT-based bot accounts; skip known bot logins (M2).
BOT_LOGINS = frozenset({'ydbot', 'github-actions'})

_LOG = logging.getLogger('manual_unmute')


def grace_ttl_calendar_days(mute_window_days, manual_unmute_window_days):
    """Calendar days a ``fast_unmute_grace`` row is kept (same rule as ``expire_fast_unmute_grace``).

    Stored on insert so dashboards and TTL stay interpretable even if ``mute_config.json`` changes later.
    """
    return max(1, int(mute_window_days) - int(manual_unmute_window_days))


def load_config():
    """Fast-track window/min-runs — same keys as ``mute.constants`` / ``mute_config.json``."""
    return {
        'window_days': get_manual_unmute_window_days(),
        'min_runs': get_manual_unmute_min_runs(),
    }


def _delete_fast_unmute_row_and_grace(
    ydb_wrapper, table_path, grace_table_path, full_name, branch, build_type, *, log_prefix
):
    """Remove one ``fast_unmute_active`` row and best-effort matching ``fast_unmute_grace`` row."""
    delete_row(ydb_wrapper, table_path, full_name, branch, build_type)
    if not grace_table_path:
        return
    try:
        delete_grace_row(ydb_wrapper, grace_table_path, full_name, branch, build_type)
    except Exception as exc:
        logging.warning(
            '%s: grace delete %s %s %s: %s',
            log_prefix,
            full_name,
            branch,
            build_type,
            exc,
        )


def abandon_fast_unmute_if_issue_not_completed(ydb_wrapper, table_path, grace_table_path):
    """Drop fast-unmute rows when the issue is no longer CLOSED+COMPLETED on GitHub (e.g. reopened).

    Clears ``fast_unmute_active`` (and matching ``fast_unmute_grace`` rows when configured),
    removes the label, sets project Status → Muted, and posts ``COMMENT_ABANDON_NOT_COMPLETED``.
    """
    rows = fetch_all_rows(ydb_wrapper, table_path)
    if not rows:
        return

    by_issue = defaultdict(list)
    for row in rows:
        inn = row.get('github_issue_number')
        if inn is None:
            continue
        by_issue[int(inn)].append(row)

    if not by_issue:
        return

    states = fetch_issue_states(list(by_issue.keys()))
    run_url = workflow_run_url()
    abandoned = 0
    rows_deleted = 0

    for issue_number in sorted(by_issue.keys()):
        meta = states.get(issue_number) or {}
        issue_id = meta.get('id')
        state = meta.get('state') or ''
        state_reason = meta.get('state_reason') or ''
        if (state or '').strip().upper() == 'CLOSED' and (state_reason or '').strip().upper() == 'COMPLETED':
            continue
        if not issue_id:
            logging.warning(
                'manual_unmute_abandon: issue #%s has fast-unmute rows but no GitHub node id; skip',
                issue_number,
            )
            continue

        for row in by_issue[issue_number]:
            fn, br, bt = row.get('full_name'), row.get('branch'), row.get('build_type')
            if not fn or not br or not bt:
                continue
            _delete_fast_unmute_row_and_grace(
                ydb_wrapper, table_path, grace_table_path, fn, br, bt, log_prefix='manual_unmute_abandon'
            )
            rows_deleted += 1

        remove_label_from_issue(issue_id)
        set_manual_unmute_project_board_status(issue_id, PROJECT_STATUS_ON_FAST_UNMUTE_FAIL)
        add_issue_comment(
            issue_id,
            COMMENT_ABANDON_NOT_COMPLETED.format(workflow_run_url=run_url),
        )
        abandoned += 1

    if abandoned:
        logging.info(
            'manual_unmute_abandon: cleared %d issue(s), %d fast_unmute row(s) (issue not CLOSED+COMPLETED)',
            abandoned,
            rows_deleted,
        )


def workflow_run_url():
    server = os.environ.get('GITHUB_SERVER_URL', 'https://github.com')
    repo = os.environ.get('GITHUB_REPOSITORY', '')
    run_id = os.environ.get('GITHUB_RUN_ID', '')
    if repo and run_id:
        return f"{server}/{repo}/actions/runs/{run_id}"
    return 'N/A'


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
    candidates = list(
        {int(c['issue_number']): c for c in raw_candidates if c.get('issue_number') is not None}.values()
    )
    if not candidates:
        logging.info('manual_unmute_enter: no CLOSED+COMPLETED candidates in lookback window')
        return

    issue_numbers = {int(c['issue_number']) for c in candidates if c.get('issue_number') is not None}
    in_project_issue_numbers = fetch_issue_numbers_in_manual_unmute_project(issue_numbers)
    closers = fetch_issue_closers(in_project_issue_numbers)
    labels_by_issue = fetch_issue_label_names(in_project_issue_numbers)

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
        if issue_number not in in_project_issue_numbers:
            _LOG.debug(
                'enter: skip #%s: issue is not in project %s',
                issue_number,
                PROJECT_ID,
            )
            continue

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
        labels = labels_by_issue.get(issue_number) or set()
        if MANUAL_FAST_UNMUTE_FINISHED_GITHUB_LABEL in labels:
            _LOG.debug(
                'enter: skip #%s: already has label %r',
                issue_number,
                MANUAL_FAST_UNMUTE_FINISHED_GITHUB_LABEL,
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

        set_manual_unmute_project_board_status(issue_id, PROJECT_STATUS_ON_FAST_UNMUTE_REOPEN)
        raw_login = (closer.get('login') or '').strip()
        closer_mention_line = f'@{raw_login}\n\n' if raw_login else ''
        add_issue_comment(
            issue_id,
            COMMENT_ENTER.format(
                closer_mention_line=closer_mention_line,
                closer_login=raw_login or 'unknown',
                window_days=window_days,
                min_runs=min_runs,
                tests_bullet_list=format_bullet_list(r['full_name'] for r in issue_rows),
                workflow_run_url=run_url,
            ),
        )
        add_label_to_issue(issue_id)

        new_rows.extend(issue_rows)
        for row in issue_rows:
            existing[(row['full_name'], row['branch'], row['build_type'])] = row

    logging.info(
        'manual_unmute_enter: inserted %d row(s) from %d candidate issue(s)',
        len(new_rows),
        len(candidates),
    )


def cleanup_manual_unmute(ydb_wrapper, table_path, tests_monitor_path):
    """Drop rows: unmuted in CI, or whole issue on TTL miss."""
    rows = fetch_all_rows(ydb_wrapper, table_path)
    if not rows:
        return

    now = datetime.datetime.now(tz=datetime.timezone.utc)
    run_url = workflow_run_url()
    ttl_days = get_manual_unmute_ttl_calendar_days()
    ttl_delta = datetime.timedelta(days=ttl_days)

    grouped = {}
    for row in rows:
        key = (row.get('branch'), row.get('build_type'))
        if not key[0] or not key[1]:
            continue
        grouped.setdefault(key, []).append(row)

    affected_issues = set()
    issues_cleared_via_unmute = set()
    unmuted_tests_by_issue = defaultdict(list)
    delete_count = 0
    grace_table_path = None
    try:
        grace_table_path = ydb_wrapper.get_table_path('fast_unmute_grace')
        create_fast_unmute_grace_table(ydb_wrapper, grace_table_path)
    except KeyError:
        pass

    grace_ttl_snapshot = grace_ttl_calendar_days(
        get_mute_window_days(), get_manual_unmute_window_days()
    )

    issues_ttl_shutdown = set()
    ttl_stuck_tests_by_issue = defaultdict(list)

    for (branch, build_type), group_rows in grouped.items():
        currently_muted = fetch_currently_muted(ydb_wrapper, tests_monitor_path, branch, build_type)
        for row in group_rows:
            full_name = row.get('full_name')
            if not full_name:
                continue
            requested_at = _coerce_dt(row.get('requested_at'))
            issue_number = row.get('github_issue_number')

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
                            grace_ttl_snapshot,
                        )
                    except Exception as exc:
                        logging.warning('Failed to record fast-unmute grace for %s: %s', full_name, exc)
                delete_row(ydb_wrapper, table_path, full_name, branch, build_type)
                delete_count += 1
                if issue_number:
                    inum = int(issue_number)
                    affected_issues.add(inum)
                    issues_cleared_via_unmute.add(inum)
                    unmuted_tests_by_issue[inum].append(full_name)
                logging.info('manual_unmute_cleanup: %s (already unmuted)', full_name)
                continue

            if requested_at and (now - requested_at) > ttl_delta:
                if issue_number:
                    inum = int(issue_number)
                    issues_ttl_shutdown.add(inum)
                    ttl_stuck_tests_by_issue[inum].append(full_name)
                    affected_issues.add(inum)
                logging.info(
                    'manual_unmute_cleanup: %s (ttl %s calendar days exceeded, still muted)',
                    full_name,
                    ttl_days,
                )
                continue

    ttl_bulk_removed_by_issue = defaultdict(set)
    if issues_ttl_shutdown:
        for row in fetch_all_rows(ydb_wrapper, table_path):
            inn = row.get('github_issue_number')
            if inn is None or int(inn) not in issues_ttl_shutdown:
                continue
            fn, br, bt = row.get('full_name'), row.get('branch'), row.get('build_type')
            if not fn or not br or not bt:
                continue
            delete_row(ydb_wrapper, table_path, fn, br, bt)
            delete_count += 1
            ttl_bulk_removed_by_issue[int(inn)].add(fn)
            logging.info('manual_unmute_cleanup: %s (bulk clear issue #%s after ttl)', fn, int(inn))

    if affected_issues:
        remaining = count_rows_per_issue(ydb_wrapper, table_path, affected_issues)
        issues_to_delabel = {num for num in affected_issues if remaining.get(num, 0) == 0}
        issue_ids = {
            issue_number: data['id']
            for issue_number, data in fetch_issue_states(affected_issues).items()
        }

        for issue_number in sorted(issues_ttl_shutdown):
            issue_id = issue_ids.get(issue_number)
            if not issue_id:
                logging.warning(
                    'manual_unmute: ttl shutdown for issue #%s: YDB cleared but no GitHub node id',
                    issue_number,
                )
                continue
            stuck = sorted(set(ttl_stuck_tests_by_issue.get(issue_number, [])))
            graduated = sorted(set(unmuted_tests_by_issue.get(issue_number, [])))
            bulk_all = sorted(ttl_bulk_removed_by_issue.get(issue_number, set()))
            cleared_other = sorted(set(bulk_all) - set(stuck))
            reopen_issue(issue_id)
            add_issue_comment(
                issue_id,
                COMMENT_TTL_INCOMPLETE.format(
                    ttl_days=ttl_days,
                    graduated_bullets=format_bullet_list(graduated)
                    if graduated
                    else '- _(none)_',
                    stuck_bullets=format_bullet_list(stuck) if stuck else '- _(none)_',
                    cleared_other_bullets=format_bullet_list(cleared_other)
                    if cleared_other
                    else '- _(none)_',
                    workflow_run_url=run_url,
                ),
            )
            set_manual_unmute_project_board_status(
                issue_id, PROJECT_STATUS_ON_FAST_UNMUTE_FAIL
            )

        for issue_number in sorted(issues_cleared_via_unmute):
            if remaining.get(issue_number, 0) == 0:
                continue
            if issue_number in issues_ttl_shutdown:
                continue
            issue_id = issue_ids.get(issue_number)
            if not issue_id:
                continue
            names = sorted(set(unmuted_tests_by_issue.get(issue_number, [])))
            if not names:
                continue
            add_issue_comment(
                issue_id,
                COMMENT_PROGRESS.format(
                    unmuted_bullets=format_bullet_list(names),
                    workflow_run_url=run_url,
                ),
            )

        success_comment_issues = (
            issues_to_delabel
            & issues_cleared_via_unmute
            - issues_ttl_shutdown
        )
        for issue_number in sorted(success_comment_issues):
            issue_id = issue_ids.get(issue_number)
            if not issue_id:
                continue
            add_issue_comment(
                issue_id,
                COMMENT_SUCCESS.format(workflow_run_url=run_url),
            )
            set_manual_unmute_project_board_status(
                issue_id, PROJECT_STATUS_ON_FAST_UNMUTE_SUCCESS
            )
            add_label_to_issue(issue_id, MANUAL_FAST_UNMUTE_FINISHED_GITHUB_LABEL)

        for issue_number in issues_to_delabel:
            issue_id = issue_ids.get(issue_number)
            if issue_id:
                remove_label_from_issue(issue_id)

    logging.info('manual_unmute_cleanup: removed %d row(s)', delete_count)


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

    abandon_fast_unmute_if_issue_not_completed(ydb_wrapper, table_path, grace_table_path)

    enter_manual_unmute(
        ydb_wrapper,
        table_path,
        issues_table_path,
        tests_monitor_path,
        config['window_days'],
        config['min_runs'],
    )
    cleanup_manual_unmute(ydb_wrapper, table_path, tests_monitor_path)
    if grace_table_path:
        expire_fast_unmute_grace(ydb_wrapper, grace_table_path)
