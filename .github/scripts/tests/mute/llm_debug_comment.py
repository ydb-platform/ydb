"""LLM-targeted debug-links comment for newly created mute issues.

After a mute issue is created we post one markdown comment listing the latest
failing/muted CI run for every test in the issue (job run URL, history
dashboard, stderr / stdout / log / logsdir) and attach an AI review label so
a downstream LLM workflow can pick the issue up. The comment carries a hidden
hint asking the LLM to classify the failure, find a likely root cause and
propose a fix.

Best-effort: any YDB / GitHub failure is logged but never raised — issue
creation is the primary action and must not regress because of this module.
"""

from __future__ import annotations

import datetime
import logging
import os
from typing import Dict, Sequence
from urllib.parse import quote_plus

from mute.update_mute_issues import (
    CURRENT_TEST_HISTORY_DASHBOARD,
    add_issue_comment,
    get_issue_comments,
)
from mute.fast_unmute_github import add_label_to_issue


AI_REVIEW_LABEL = 'need_ai_review'
FAILURE_LOOKBACK_DAYS = 7
MAX_COMMENT_LENGTH = 60000
_COMMENT_MARKER = '<!-- mute-llm-debug-links:v1 -->'

# CI jobs whose artifacts are meaningful for debugging mutes. Restricting to
# this allowlist keeps PR / manual reruns out of the "latest failing run".
_JOB_NAMES = (
    'Nightly-run',
    'Regression-run',
    'Regression-run_Large',
    'Regression-run_Small_and_Medium',
    'Regression-run_compatibility',
    'Regression-whitelist-run',
    'Postcommit_relwithdebinfo',
    'Postcommit_asan',
)

_LLM_HINT = (
    "<!-- LLM: please classify the failure as one of "
    "TEST_ISSUE / YDB_ISSUE / TEST_INFRA_ISSUE; try to find the root cause "
    "and the commit that most likely introduced it; estimate severity "
    "(HIGH / MEDIUM / LOW); when possible, propose a fix. "
    "If error_type is VERIFY or SANITIZER, explicitly mention the concrete problem "
    "being investigated using status_description in this exact 3-line shape: "
    "'VERIFY failed (<ISO_UTC_TIMESTAMP>):' then "
    "'<source_file>:<line>' then "
    "'<assertion/message line>'. "
    "Prefer lines like 'ydb/.../*.cpp:<line>' and requirement/assert text, and do NOT include "
    "stacktrace frames or addresses (e.g. '+0x...', function backtrace lines). -->"
)


def _is_need_ai_review_enabled() -> bool:
    value = os.getenv('ENABLE_NEED_AI_REVIEW_LABEL', '').strip().lower()
    return value in {'1', 'true', 'yes', 'on'}


def _format_ts(value) -> str:
    if isinstance(value, datetime.datetime):
        return value.astimezone(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
    if isinstance(value, int):
        return datetime.datetime.fromtimestamp(
            value / 1_000_000, tz=datetime.timezone.utc,
        ).strftime('%Y-%m-%d %H:%M:%S UTC')
    return str(value or 'unknown')


def _history_link(full_name: str, branch: str, build_type: str) -> str:
    return (
        f"{CURRENT_TEST_HISTORY_DASHBOARD}"
        f"full_name={quote_plus(f'__in_{full_name}')}"
        f"&branch={quote_plus(str(branch or 'main'))}"
        f"&build_type={quote_plus(str(build_type or ''))}"
    )


def _link(url) -> str:
    return f"[link]({url})" if url else 'n/a'


def _normalize_table_cell(value, max_len: int = 120) -> str:
    """Render a value safely inside a markdown table cell."""
    text = str(value or 'n/a')
    # Keep markdown table structure intact.
    text = text.replace('\r', ' ').replace('\n', ' ').replace('|', '\\|')
    text = ' '.join(text.split())
    if len(text) > max_len:
        text = text[: max_len - 1] + '…'
    return text


def _load_latest_failures(ydb_wrapper, branch, build_type, full_names) -> Dict[str, Dict]:
    """Return ``full_name -> latest failing/muted CI run row`` from ``test_history_fast``.

    Returns ``{}`` (and logs a warning) when YDB is unavailable; callers still
    render the table with history links for every test.
    """
    if ydb_wrapper is None:
        return {}
    pairs = []
    target = set(full_names)
    for name in target:
        if '/' not in name:
            continue
        suite, test = name.rsplit('/', 1)
        pairs.append((suite.replace("'", "''"), test.replace("'", "''")))
    if not pairs:
        return {}

    try:
        table_path = ydb_wrapper.get_table_path('test_history_fast')
    except (KeyError, AttributeError):
        logging.warning('test_history_fast not registered in ydb_qa_config — debug links disabled')
        return {}

    branch_esc = str(branch).replace("'", "''")
    bt_esc = str(build_type).replace("'", "''")
    jobs_in = ', '.join(f"'{j}'" for j in _JOB_NAMES)
    pair_pred = ' OR '.join(
        f"(suite_folder = '{s}' AND test_name = '{t}')" for s, t in pairs
    )
    query = f"""
    SELECT suite_folder, test_name, job_id, status, error_type, status_description, run_timestamp,
           stderr, stdout, log, logsdir
    FROM `{table_path}`
    WHERE branch = '{branch_esc}'
        AND build_type = '{bt_esc}'
        AND job_name IN ({jobs_in})
        AND (pull IS NULL OR NOT String::Contains(pull, 'manual'))
        AND ({pair_pred})
        AND status IN ('failure', 'error', 'mute')
        AND run_timestamp >= CurrentUtcTimestamp() - {FAILURE_LOOKBACK_DAYS} * Interval("P1D")
    ORDER BY run_timestamp DESC
    """
    try:
        rows = ydb_wrapper.execute_scan_query(query, query_name='mute_issue_llm_debug_links')
    except Exception as exc:
        logging.warning('Failed to load debug links from test_history_fast: %s', exc)
        return {}

    latest: Dict[str, Dict] = {}
    for row in rows:
        suite = row.get('suite_folder')
        test = row.get('test_name')
        if not suite or not test:
            continue
        full_name = f"{suite}/{test}"
        if full_name not in target or full_name in latest:
            continue
        latest[full_name] = row
    return latest


def _build_comment(full_names: Sequence[str], rows: Dict[str, Dict],
                   branch: str, build_type: str) -> str:
    lines = [
        '### Links to recent failed test runs',
        _LLM_HINT,
        _COMMENT_MARKER,
        '',
        '| test | status | type | last_run | history | run | stderr | stdout | log | logsdir |',
        '|---|---|---|---|---|---|---|---|---|---|',
    ]
    for full_name in sorted(set(full_names)):
        row = rows.get(full_name)
        history = _history_link(full_name, branch, build_type)
        short = full_name.rsplit('/', 1)[-1]
        if not row:
            lines.append(
                f"| `{short}` | n/a | n/a | n/a | {_link(history)} | n/a | n/a | n/a | n/a | n/a |"
            )
            continue
        job_id = row.get('job_id')
        run_url = f"https://github.com/ydb-platform/ydb/actions/runs/{job_id}" if job_id else ''
        type_text = _normalize_table_cell(row.get('error_type') or 'n/a', max_len=48).upper()
        lines.append(
            f"| `{short}` "
            f"| `{_normalize_table_cell(row.get('status') or 'unknown', max_len=32)}` "
            f"| `{type_text}` "
            f"| `{_format_ts(row.get('run_timestamp'))}` "
            f"| {_link(history)} "
            f"| {_link(run_url)} "
            f"| {_link(row.get('stderr'))} "
            f"| {_link(row.get('stdout'))} "
            f"| {_link(row.get('log'))} "
            f"| {_link(row.get('logsdir'))} |"
        )

    body = '\n'.join(lines)
    if len(body) <= MAX_COMMENT_LENGTH:
        return body
    # Truncate on a line boundary so we never split a markdown table row.
    suffix = '\n\n_… truncated to fit GitHub comment size limit._'
    budget = MAX_COMMENT_LENGTH - len(suffix)
    cutoff = body.rfind('\n', 0, budget)
    if cutoff <= 0:
        cutoff = budget
    return body[:cutoff] + suffix


def _already_posted(issue_id: str) -> bool:
    """Best-effort duplicate guard by hidden marker in issue comments."""
    try:
        comments = get_issue_comments(issue_id)
    except Exception as exc:
        logging.warning('Failed to check existing comments for %s: %s', issue_id, exc)
        return False
    return any(_COMMENT_MARKER in str(comment or '') for comment in comments)


def post_llm_debug_comment(ydb_wrapper, issue_id: str, issue_url: str,
                           full_names: Sequence[str], branch: str, build_type: str) -> None:
    """Post the debug-links comment and attach the AI review label. Best-effort."""
    full_names = [n for n in full_names if n]
    if not issue_id or not full_names:
        return
    try:
        rows = _load_latest_failures(ydb_wrapper, branch, build_type, full_names)
        if _already_posted(issue_id):
            logging.info('LLM debug comment already exists for %s, skipping duplicate post', issue_url)
        else:
            add_issue_comment(issue_id, _build_comment(full_names, rows, branch, build_type))
    except Exception as exc:
        logging.warning('Failed to post LLM debug comment to %s: %s', issue_url, exc)
        return
    if not _is_need_ai_review_enabled():
        return
    try:
        # add_label_to_issue caches the label id and silently no-ops on most failures.
        add_label_to_issue(issue_id, AI_REVIEW_LABEL)
    except Exception as exc:
        logging.warning('Failed to add AI review label to %s: %s', issue_url, exc)
