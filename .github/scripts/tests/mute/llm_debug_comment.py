"""LLM-targeted debug-links comment for newly created mute issues.

Posts one (or several, if it doesn't fit GitHub's comment size limit) markdown
comments to a freshly-created mute issue, listing for each test the most recent
failing/muted CI run and the artifact links available in ``test_results``
(stderr / stdout / log / logsdir / job run URL / history dashboard).

The comment is wrapped in a hidden HTML hint that asks any LLM reviewing the
issue to classify the failure (TEST_ISSUE / YDB_ISSUE / TEST_INFRA_ISSUE),
locate the most likely root cause and offending commit, and propose a fix.

After the comment is posted the configured GitHub label is attached so an AI
review workflow can pick the issue up.

This module is **best-effort**: any failure (missing table, invalid token,
network issue, etc.) is logged as a warning but never raises — issue creation
is the primary action and must not regress because the LLM annotation failed.
"""

from __future__ import annotations

import datetime
import logging
import re
from typing import Dict, Iterable, List, Optional, Sequence
from urllib.parse import quote_plus

from mute.update_mute_issues import (
    CURRENT_TEST_HISTORY_DASHBOARD,
    add_issue_comment,
    add_issue_label,
)


DEFAULT_LABEL = 'needs-ai-review'
DEFAULT_FAILURE_LOOKBACK_DAYS = 7
DEFAULT_MAX_COMMENT_LENGTH = 60000

# CI jobs that publish results to ``test_results`` and whose artifacts are
# meaningful for debugging mutes. We deliberately keep this list small and
# explicit instead of accepting any job_name — random PR jobs would pollute
# the "latest failing run" picked for a mute.
DEFAULT_JOB_NAMES: Sequence[str] = (
    'Nightly-run',
    'Regression-run',
    'Regression-run_Large',
    'Regression-run_Small_and_Medium',
    'Regression-run_compatibility',
    'Regression-whitelist-run',
    'Postcommit_relwithdebinfo',
    'Postcommit_asan',
)

_GITHUB_RUN_URL_TEMPLATE = "https://github.com/ydb-platform/ydb/actions/runs/{job_id}"

_SANITIZER_PATTERNS = (
    r'(ERROR|WARNING|SUMMARY): '
    r'(AddressSanitizer|MemorySanitizer|ThreadSanitizer|LeakSanitizer|UndefinedBehaviorSanitizer)',
    r'==\d+==\s*(ERROR|WARNING|SUMMARY): '
    r'(AddressSanitizer|MemorySanitizer|ThreadSanitizer|LeakSanitizer|UndefinedBehaviorSanitizer)',
    r'runtime error:',
    r'detected memory leaks',
)
_SANITIZER_RE = re.compile('|'.join(_SANITIZER_PATTERNS), re.IGNORECASE | re.MULTILINE)

_HEADER = '### Debug links (latest failed run)'

# Hidden hint for the LLM; not rendered in GitHub UI but available in the raw
# comment body that an AI agent fetches via the API.
_LLM_HINT = (
    "<!-- LLM: please classify the failure as one of "
    "TEST_ISSUE / YDB_ISSUE / TEST_INFRA_ISSUE; try to find the root cause "
    "and the commit that most likely introduced it; estimate severity "
    "(HIGH / MEDIUM / LOW); when possible, propose a fix. -->"
)


def _sql_str_in(values: Iterable[str]) -> str:
    """Render a sequence of strings as a comma-separated SQL ``IN`` clause body."""
    escaped = [str(v).replace("'", "''") for v in values if v is not None]
    return ', '.join(f"'{v}'" for v in escaped)


def _format_run_timestamp(value) -> str:
    if value is None:
        return 'unknown'
    if isinstance(value, datetime.datetime):
        return value.astimezone(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
    if isinstance(value, datetime.date):
        return value.strftime('%Y-%m-%d')
    if isinstance(value, int):
        return datetime.datetime.fromtimestamp(
            value / 1_000_000, tz=datetime.timezone.utc
        ).strftime('%Y-%m-%d %H:%M:%S UTC')
    return str(value)


def _is_sanitizer_issue(error_text: object) -> bool:
    return bool(error_text) and bool(_SANITIZER_RE.search(str(error_text)))


def _normalize_error_type(error_type, status_description) -> str:
    if _is_sanitizer_issue(status_description):
        return 'SANITIZER'
    return str(error_type or '').upper() or 'n/a'


def _split_full_name(full_name: str) -> Optional[tuple]:
    if not full_name or '/' not in full_name:
        return None
    suite_folder, test_name = full_name.rsplit('/', 1)
    return suite_folder, test_name


def _compose_full_name_from_row(row: Dict) -> str:
    full_name = row.get('full_name')
    if full_name:
        return str(full_name)
    suite_folder = row.get('suite_folder')
    test_name = row.get('test_name')
    if suite_folder and test_name:
        return f"{suite_folder}/{test_name}"
    return ''


def _history_link(full_name: str, branch: str, build_type: str) -> str:
    return (
        f"{CURRENT_TEST_HISTORY_DASHBOARD}"
        f"full_name={quote_plus(f'__in_{full_name}')}"
        f"&branch={quote_plus(str(branch or 'main'))}"
        f"&build_type={quote_plus(str(build_type or ''))}"
    )


def _md_escape_cell(value: object) -> str:
    return str(value).replace('|', '\\|')


def load_latest_failure_links(
    ydb_wrapper,
    branch: str,
    build_type: str,
    full_names: Sequence[str],
    *,
    window_days: int = DEFAULT_FAILURE_LOOKBACK_DAYS,
    job_names: Sequence[str] = DEFAULT_JOB_NAMES,
) -> Dict[str, Dict]:
    """Fetch the most recent failing/muted run for each ``full_name``.

    Returns a mapping ``full_name -> {job_id, run_url, status, error_type,
    status_description, run_timestamp, duration, stderr, stdout, log, logsdir}``.

    Only failures with explicit job names from ``job_names`` are considered, and
    PR-manual reruns (``pull`` LIKE ``%manual%``) are filtered out.
    """
    full_names = [str(n) for n in full_names if n]
    if not full_names:
        return {}

    try:
        table_path = ydb_wrapper.get_table_path('test_results')
    except KeyError:
        logging.warning('test_results table is not registered in ydb_qa_config — debug links disabled')
        return {}

    pairs = []
    target_names = set(full_names)
    for name in target_names:
        parts = _split_full_name(name)
        if parts is None:
            continue
        suite, test = parts
        pairs.append((suite.replace("'", "''"), test.replace("'", "''")))
    if not pairs:
        return {}

    branch_esc = str(branch).replace("'", "''")
    bt_esc = str(build_type).replace("'", "''")
    jobs_in_sql = _sql_str_in(job_names)
    pair_predicate = ' OR '.join(
        f"(suite_folder = '{suite}' AND test_name = '{test}')" for suite, test in pairs
    )
    query = f"""
    SELECT
        suite_folder, test_name, job_id, status, error_type, status_description,
        duration, run_timestamp, stderr, stdout, log, logsdir
    FROM `{table_path}`
    WHERE branch = '{branch_esc}'
        AND build_type = '{bt_esc}'
        AND job_name IN ({jobs_in_sql})
        AND (pull IS NULL OR NOT String::Contains(pull, 'manual'))
        AND ({pair_predicate})
        AND status IN ('failure', 'error', 'mute')
        AND run_timestamp >= CurrentUtcTimestamp() - {int(window_days)} * Interval("P1D")
    ORDER BY run_timestamp DESC
    """
    try:
        rows = ydb_wrapper.execute_scan_query(query, query_name='mute_issue_llm_debug_links')
    except Exception as exc:
        logging.warning('Failed to load debug links from test_results: %s', exc)
        return {}

    latest: Dict[str, Dict] = {}
    for row in rows:
        full_name = _compose_full_name_from_row(row)
        if full_name not in target_names or full_name in latest:
            continue
        job_id = row.get('job_id')
        latest[full_name] = {
            'job_id': job_id,
            'run_url': _GITHUB_RUN_URL_TEMPLATE.format(job_id=job_id) if job_id else '',
            'test_name': row.get('test_name'),
            'status': row.get('status') or 'unknown',
            'error_type': row.get('error_type') or '',
            'status_description': row.get('status_description') or '',
            'duration': row.get('duration'),
            'run_timestamp': _format_run_timestamp(row.get('run_timestamp')),
            'stderr': row.get('stderr'),
            'stdout': row.get('stdout'),
            'log': row.get('log'),
            'logsdir': row.get('logsdir'),
        }
    return latest


def _format_duration(value) -> str:
    if value is None:
        return 'n/a'
    try:
        return f"`{float(value):.2f}s`"
    except (TypeError, ValueError):
        return f"`{_md_escape_cell(value)}`"


def _link_cell(url) -> str:
    return f"[link]({url})" if url else 'n/a'


def build_debug_links_table(
    full_names: Sequence[str],
    debug_links: Dict[str, Dict],
    branch: str,
    build_type: str,
) -> str:
    """Build the markdown table body (without the header / hint)."""
    lines = [
        "| test_name | status | type | duration | last_run | history | run | stderr | stdout | log | logsdir |",
        "|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for full_name in sorted({str(n) for n in full_names if n}):
        data = debug_links.get(full_name)
        short_name = full_name.rsplit('/', 1)[-1] if '/' in full_name else full_name
        if data and data.get('test_name'):
            short_name = str(data['test_name'])
        name_cell = f"`{_md_escape_cell(short_name)}`"
        history = _history_link(full_name, branch, build_type)

        if not data:
            lines.append(
                f"| {name_cell} | n/a | n/a | n/a | n/a | "
                f"{_link_cell(history)} | n/a | n/a | n/a | n/a | n/a |"
            )
            continue

        lines.append(
            "| {name} | `{status}` | `{etype}` | {duration} | `{ts}` | "
            "{history} | {run} | {stderr} | {stdout} | {log} | {logsdir} |".format(
                name=name_cell,
                status=_md_escape_cell(data.get('status') or 'unknown'),
                etype=_md_escape_cell(_normalize_error_type(
                    data.get('error_type'), data.get('status_description'),
                )),
                duration=_format_duration(data.get('duration')),
                ts=_md_escape_cell(data.get('run_timestamp') or 'unknown'),
                history=_link_cell(history),
                run=_link_cell(data.get('run_url')),
                stderr=_link_cell(data.get('stderr')),
                stdout=_link_cell(data.get('stdout')),
                log=_link_cell(data.get('log')),
                logsdir=_link_cell(data.get('logsdir')),
            )
        )

    if not any(debug_links.values()):
        lines.append("")
        lines.append("_No stderr/stdout/log/logsdir links found in recent failed runs._")
    return "\n".join(lines)


def split_into_comments(table_body: str, max_length: int = DEFAULT_MAX_COMMENT_LENGTH) -> List[str]:
    """Wrap ``table_body`` with header + LLM hint, splitting into multiple comments if needed.

    Splits on table rows so each chunk stays valid markdown.
    """
    body = (table_body or '').strip()
    prefix_overhead = len(_HEADER) + 1 + len(_LLM_HINT) + 1
    if not body:
        return [f"{_HEADER}\n{_LLM_HINT}"]

    rows = body.split('\n')
    # Header rows of the markdown table: title row + separator row.
    if len(rows) >= 2 and rows[0].startswith('|') and set(rows[1].replace('|', '').strip()) <= {'-', ':', ' '}:
        table_header = '\n'.join(rows[:2])
        data_rows = rows[2:]
    else:
        table_header = ''
        data_rows = rows

    chunks: List[str] = []
    current_rows: List[str] = []
    current_len = 0
    base_chunk_len = (len(table_header) + 1) if table_header else 0

    def _flush() -> None:
        nonlocal current_rows, current_len
        if not current_rows:
            return
        block = '\n'.join(([table_header] if table_header else []) + current_rows)
        chunks.append(block)
        current_rows = []
        current_len = 0

    for row in data_rows:
        row_len = len(row) + 1
        if current_rows and (
            prefix_overhead + base_chunk_len + current_len + row_len > max_length
        ):
            _flush()
        current_rows.append(row)
        current_len += row_len
    _flush()

    if not chunks:
        chunks = [body]

    if len(chunks) == 1:
        return [f"{_HEADER}\n{_LLM_HINT}\n{chunks[0]}"]

    total = len(chunks)
    return [
        f"{_HEADER} (part {idx}/{total})\n{_LLM_HINT}\n{chunk}"
        for idx, chunk in enumerate(chunks, start=1)
    ]


def post_llm_debug_comment(
    ydb_wrapper,
    issue_id: str,
    issue_url: str,
    full_names: Sequence[str],
    branch: str,
    build_type: str,
    *,
    label: str = DEFAULT_LABEL,
    window_days: int = DEFAULT_FAILURE_LOOKBACK_DAYS,
    max_comment_length: int = DEFAULT_MAX_COMMENT_LENGTH,
    job_names: Sequence[str] = DEFAULT_JOB_NAMES,
) -> bool:
    """Post the LLM-targeted debug-links comment(s) and attach the AI review label.

    Best-effort: returns ``True`` when at least the comment was posted; logs and
    returns ``False`` on any failure without raising.
    """
    if not issue_id:
        logging.warning('post_llm_debug_comment: empty issue_id, skipping')
        return False
    full_names = [str(n) for n in full_names if n]
    if not full_names:
        return False

    try:
        debug_links = load_latest_failure_links(
            ydb_wrapper,
            branch=branch,
            build_type=build_type,
            full_names=full_names,
            window_days=window_days,
            job_names=job_names,
        )
        table_body = build_debug_links_table(full_names, debug_links, branch, build_type)
        chunks = split_into_comments(table_body, max_length=max_comment_length)
        for chunk in chunks:
            add_issue_comment(issue_id, chunk)
    except Exception as exc:
        logging.warning('Failed to post LLM debug comment to %s: %s', issue_url, exc)
        return False

    try:
        add_issue_label(issue_id, label)
    except Exception as exc:
        logging.warning('Failed to add label %r to %s: %s', label, issue_url, exc)
    return True
