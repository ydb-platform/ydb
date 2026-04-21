#!/usr/bin/env python3

"""Manual fast-unmute state machine.

When a user manually closes a mute issue as Completed, every listed test that
is still muted in CI gets a row in ``fast_unmute_active``. While a row exists,
``mute/create_new_muted_ya.py`` uses the short window from ``mute_config.json``.

**During TTL** (``manual_unmute_ttl_calendar_days``): if a test becomes unmuted
in monitor data, only that row is removed; automation may post a short
**progress** comment while other tests on the same issue are still tracked.

**Before TTL ends**, if every tracked test for the issue has left ``is_muted`` in
CI, all rows are gone → **success** comment, org project **Status → Unmuted**,
``manual-fast-unmute`` label removed (issue stays closed).

**After TTL**: if any tracked row is still muted, **all** rows for that issue are
removed, the issue is **reopened**, **Status → Muted**, one **deadline** comment
(lists who already unmuted vs who is still muted vs tracking cleared early),
label removed.

``create_new_muted_ya`` still decides short-window unmute; rows in
``fast_unmute_active`` are removed only when the test is unmuted in CI or when
the TTL path runs for the issue.

Entering fast-unmute does **not** reopen the issue (issue stays closed); tracking
uses YDB and the ``manual-fast-unmute`` label.

If someone **closes the issue again** as Completed while tests are still muted,
the next ``sync`` can insert new rows and start another fast-unmute cycle.

Usage:
    python3 .github/scripts/tests/mute/manual_unmute.py sync
    python3 .github/scripts/tests/mute/manual_unmute.py sync -v   # DEBUG: why each issue/test was skipped

Testing only — unset before real runs.

``MANUAL_UNMUTE_SIMULATE_UNMUTED``: comma-separated ``full_name`` values removed from the
"currently muted" set so cleanup behaves as if ``is_muted`` were already 0 (grace path).

``export MANUAL_UNMUTE_SIMULATE_UNMUTED='suite/path/Test.one' && python3 .github/scripts/tests/mute/manual_unmute.py sync``
"""

import argparse
import datetime
import logging
import os
import sys
from collections import defaultdict

import ydb

_mutedir = os.path.dirname(os.path.abspath(__file__))
_tests_dir = os.path.dirname(_mutedir)
_scripts_dir = os.path.dirname(_tests_dir)
for _p in (_scripts_dir, os.path.join(_scripts_dir, 'analytics'), _tests_dir):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from github_issue_utils import DEFAULT_BUILD_TYPE, parse_body
from mute.update_mute_issues import (
    MANUAL_FAST_UNMUTE_GITHUB_LABEL,
    ORG_NAME,
    PROJECT_ID,
    REPO_NAME,
    add_issue_comment,
    get_project_v2_fields,
    run_query,
)
from mute.constants import (
    get_manual_unmute_currently_muted_lookback_days,
    get_manual_unmute_issue_closed_lookback_days,
    get_manual_unmute_min_runs,
    get_manual_unmute_ttl_calendar_days,
    get_manual_unmute_window_days,
    get_mute_window_days,
)
from ydb_wrapper import YDBWrapper


def grace_ttl_calendar_days(mute_window_days, manual_unmute_window_days):
    """Calendar days a ``fast_unmute_grace`` row is kept (same rule as ``expire_fast_unmute_grace``).

    Stored on insert so dashboards and TTL stay interpretable even if ``mute_config.json`` changes later.
    """
    return max(1, int(mute_window_days) - int(manual_unmute_window_days))

LABEL_NAME = MANUAL_FAST_UNMUTE_GITHUB_LABEL

# Org project board (same as ``mute.update_mute_issues.PROJECT_ID``).
PROJECT_STATUS_ON_FAST_UNMUTE_REOPEN = 'Observation'
PROJECT_STATUS_ON_FAST_UNMUTE_FAIL = 'Muted'
PROJECT_STATUS_ON_FAST_UNMUTE_SUCCESS = 'Unmuted'

_LABEL_ID_CACHE = {}

# GitHub ``__typename`` is ``User`` for PAT-based bot accounts; skip known bot logins (M2).
BOT_LOGINS = frozenset({'ydbot', 'github-actions'})

# Verbose (`sync -v`) touches only this logger so ydb/grpc stay at INFO (no RPC spam).
_LOG = logging.getLogger('manual_unmute')

# Local testing: comma-separated ``tests_monitor.full_name`` — excluded from ``fetch_currently_muted``.
_SIMULATE_UNMUTED_ENV = 'MANUAL_UNMUTE_SIMULATE_UNMUTED'


def _simulate_unmuted_full_names():
    raw = os.environ.get(_SIMULATE_UNMUTED_ENV, '').strip()
    if not raw:
        return frozenset()
    return frozenset(x.strip() for x in raw.split(',') if x.strip())


COMMENT_ENTER = """🚀 **Fast-unmute started**

{closer_mention_line}You closed this issue as completed. The issue stays **closed**; automation registered fast-unmute in YDB and added the `manual-fast-unmute` label.

The listed tests are still muted in the repo, but CI now evaluates them on a **shorter unmute window** ({window_days} calendar days, min {min_runs} clean runs) until they qualify for automatic unmute:

{tests_bullet_list}

**What happens next**

- **While time runs** (``manual_unmute_ttl_calendar_days`` calendar days per row from registration): if a test is already **unmuted in CI data**, only its fast-unmute row is removed; you may see a short **progress** comment if other tests on this issue are still tracked.
- **All tests unmuted in CI before anyone hits the deadline** → success comment, project **Status → Unmuted**, fast-unmute label removed (this issue stays **closed**).
- **After the deadline**, if any test is **still muted** in CI → this issue is **reopened**, **Status → Muted**, all fast-unmute rows for this issue are cleared, one summary comment, label removed.
- No action needed from you. Please do not edit `muted_ya.txt` manually.

🔗 Workflow run: {workflow_run_url}
"""


COMMENT_SUCCESS = """✅ **Fast-unmute completed**

All tests from this issue are **no longer muted** in CI data before the fast-unmute deadline. They will be removed from `muted_ya` on the next mute automation run that updates the repo — no action needed from you.

Project **Status** → **Unmuted**. The `manual-fast-unmute` label is removed.

🔗 Workflow run: {workflow_run_url}
"""


COMMENT_PROGRESS = """📌 **Fast-unmute: progress**

While the deadline is still running for other tests on this issue, these are **already unmuted** in CI data — the fast-unmute row was removed only for them:

{unmuted_bullets}

🔗 Workflow run: {workflow_run_url}
"""


COMMENT_TTL_INCOMPLETE = """⏱️ **Fast-unmute: deadline passed**

The fast-unmute calendar limit (**{ttl_days}** days from row registration) has passed, but at least one test is **still muted** in CI. Fast-unmute tracking for **this whole issue** is cleared, the issue is **reopened**, project **Status** → **Muted**, and the `manual-fast-unmute` label is removed.

**Already unmuted in CI (pending `muted_ya` update):**
{graduated_bullets}

**Still muted after the deadline:**
{stuck_bullets}

**Tracking cleared early (same issue, deadline not reached for that row):**
{cleared_other_bullets}

🔗 Workflow run: {workflow_run_url}
"""


def load_config():
    """Fast-track window/min-runs — same keys as ``mute.constants`` / ``mute_config.json``."""
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
    if isinstance(value, datetime.date):
        return datetime.datetime.combine(value, datetime.time.min, tzinfo=datetime.timezone.utc)
    if isinstance(value, int):
        return datetime.datetime.fromtimestamp(value / 1_000_000, tz=datetime.timezone.utc)
    if isinstance(value, float):
        return datetime.datetime.fromtimestamp(value, tz=datetime.timezone.utc)
    return None


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
    result = {row['full_name'] for row in rows if row.get('full_name')}
    pretend_unmuted = _simulate_unmuted_full_names()
    if pretend_unmuted:
        logging.warning(
            '%s active — excluding from currently-muted (simulate is_muted=0): %s',
            _SIMULATE_UNMUTED_ENV,
            ', '.join(sorted(pretend_unmuted)),
        )
        result -= pretend_unmuted
    return result


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
                    timelineItems(last: 1, itemTypes: [CLOSED_EVENT]) {{
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
                # ``last: 1`` returns the most recent close; ``nodes[0]`` is that event.
                events = (node.get('timelineItems') or {}).get('nodes') or []
                event = events[0] if events else {}
                actor = event.get('actor') or {}
                login = actor.get('login') or ''
                actor_type = actor.get('__typename') or ''
            result[number] = {'login': login, 'type': actor_type}
    return result


def reopen_issue(issue_id):
    """Reopen a closed issue. No-op if already open."""
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


def _issue_project_board_item_id(issue_node_id, project_number):
    """Return Project v2 **item** id for ``issue_node_id`` on board ``project_number``, or ``None``."""
    query = """
    query ($issueId: ID!) {
      node(id: $issueId) {
        ... on Issue {
          projectItems(first: 40) {
            nodes {
              id
              project { number }
            }
          }
        }
      }
    }
    """
    try:
        result = run_query(query, {'issueId': issue_node_id})
    except Exception as exc:
        logging.warning('manual_unmute: projectItems query failed: %s', exc)
        return None
    node = (result.get('data') or {}).get('node') or {}
    want = int(project_number)
    for it in ((node.get('projectItems') or {}).get('nodes')) or []:
        num = (it.get('project') or {}).get('number')
        if num is not None and int(num) == want:
            return it.get('id')
    return None


def _add_issue_to_org_project(project_global_id, issue_node_id):
    """Add issue to org project; return new project **item** id."""
    mutation = """
    mutation ($projectId: ID!, $contentId: ID!) {
      addProjectV2ItemById(input: {projectId: $projectId, contentId: $contentId}) {
        item { id }
      }
    }
    """
    try:
        result = run_query(
            mutation, {'projectId': project_global_id, 'contentId': issue_node_id}
        )
    except Exception as exc:
        logging.warning('manual_unmute: addProjectV2ItemById failed: %s', exc)
        return None
    item = (((result.get('data') or {}).get('addProjectV2ItemById') or {}).get('item') or {})
    return item.get('id')


def _set_manual_unmute_project_board_status(issue_node_id, status_label):
    """Set org project ``Status`` (single select) by option name (case-insensitive).

    Issues not yet on the board are added to the project (same behaviour as mute tooling).
    Requires token scope that can read/update org projects.
    """
    label = (status_label or '').strip()
    if not label:
        return
    try:
        project_global_id, project_fields = get_project_v2_fields(ORG_NAME, PROJECT_ID)
    except Exception as exc:
        logging.warning('manual_unmute: could not load project %s fields: %s', PROJECT_ID, exc)
        return
    status_field_id = None
    option_id = None
    want = label.lower()
    for field in project_fields:
        if (field.get('name') or '').lower() != 'status':
            continue
        status_field_id = field.get('id')
        for opt in field.get('options') or []:
            if (opt.get('name') or '').lower() == want:
                option_id = opt.get('id')
                break
        break
    if not status_field_id or not option_id:
        logging.warning(
            'manual_unmute: project %s: Status field or %r option not found; skip board update',
            PROJECT_ID,
            label,
        )
        return

    item_id = _issue_project_board_item_id(issue_node_id, PROJECT_ID)
    if not item_id:
        item_id = _add_issue_to_org_project(project_global_id, issue_node_id)
    if not item_id:
        logging.warning(
            'manual_unmute: could not resolve or create project item for issue (project %s)',
            PROJECT_ID,
        )
        return

    mutation = """
    mutation ($projectId: ID!, $itemId: ID!, $fieldId: ID!, $optionId: String) {
      updateProjectV2ItemFieldValue(input: {
        projectId: $projectId,
        itemId: $itemId,
        fieldId: $fieldId,
        value: { singleSelectOptionId: $optionId }
      }) {
        projectV2Item { id }
      }
    }
    """
    try:
        run_query(
            mutation,
            {
                'projectId': project_global_id,
                'itemId': item_id,
                'fieldId': status_field_id,
                'optionId': option_id,
            },
        )
        logging.info(
            'manual_unmute: set project %s Status to %r',
            PROJECT_ID,
            label,
        )
    except Exception as exc:
        logging.warning('manual_unmute: updateProjectV2ItemFieldValue failed: %s', exc)


def set_fast_unmute_reopen_project_status(issue_node_id):
    """Set org project Status → Observation when entering fast-unmute."""
    _set_manual_unmute_project_board_status(
        issue_node_id, PROJECT_STATUS_ON_FAST_UNMUTE_REOPEN
    )


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
        `grace_ttl_days`              Uint32    NOT NULL,
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
    grace_ttl_days,
):
    column_types = (
        ydb.BulkUpsertColumns()
        .add_column('full_name', ydb.PrimitiveType.Utf8)
        .add_column('branch', ydb.PrimitiveType.Utf8)
        .add_column('build_type', ydb.PrimitiveType.Utf8)
        .add_column('github_issue_number', ydb.PrimitiveType.Uint64)
        .add_column('fast_track_requested_at', ydb.PrimitiveType.Timestamp)
        .add_column('grace_started_at', ydb.PrimitiveType.Timestamp)
        .add_column('grace_ttl_days', ydb.PrimitiveType.Uint32)
    )
    rows = [
        {
            'full_name': full_name,
            'branch': branch,
            'build_type': build_type,
            'github_issue_number': int(github_issue_number),
            'fast_track_requested_at': fast_track_requested_at,
            'grace_started_at': grace_started_at,
            'grace_ttl_days': int(grace_ttl_days),
        }
    ]
    ydb_wrapper.bulk_upsert(table_path, rows, column_types)


def expire_fast_unmute_grace(ydb_wrapper, table_path):
    """Remove grace rows after ``grace_ttl_days`` calendar days since ``grace_started_at``."""
    query = f"""
    SELECT full_name, branch, build_type, grace_started_at, grace_ttl_days
    FROM `{table_path}`
    """
    try:
        rows = ydb_wrapper.execute_scan_query(query, query_name='fast_unmute_grace_expire_scan')
    except Exception as exc:
        logging.warning('expire_fast_unmute_grace: scan failed: %s', exc)
        return

    today = datetime.datetime.now(tz=datetime.timezone.utc).date()

    for row in rows:
        gs = _coerce_dt(row.get('grace_started_at'))
        if not gs:
            continue
        gs_date = gs.astimezone(datetime.timezone.utc).date()
        threshold = max(1, int(row['grace_ttl_days']))
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

        set_fast_unmute_reopen_project_status(issue_id)
        raw_login = (closer.get('login') or '').strip()
        closer_mention_line = f'@{raw_login}\n\n' if raw_login else ''
        add_issue_comment(
            issue_id,
            COMMENT_ENTER.format(
                closer_mention_line=closer_mention_line,
                closer_login=raw_login or 'unknown',
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

    grace_ttl_snapshot = grace_ttl_calendar_days(get_mute_window_days(), get_manual_unmute_window_days())

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
        issue_ids = _fetch_issue_node_ids(affected_issues)

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
                    graduated_bullets=_format_bullet_list(graduated)
                    if graduated
                    else '- _(none)_',
                    stuck_bullets=_format_bullet_list(stuck) if stuck else '- _(none)_',
                    cleared_other_bullets=_format_bullet_list(cleared_other)
                    if cleared_other
                    else '- _(none)_',
                    workflow_run_url=run_url,
                ),
            )
            _set_manual_unmute_project_board_status(
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
                    unmuted_bullets=_format_bullet_list(names),
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
            _set_manual_unmute_project_board_status(
                issue_id, PROJECT_STATUS_ON_FAST_UNMUTE_SUCCESS
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
    cleanup_manual_unmute(ydb_wrapper, table_path, tests_monitor_path)
    if grace_table_path:
        expire_fast_unmute_grace(ydb_wrapper, grace_table_path)


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
