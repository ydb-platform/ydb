#!/usr/bin/env python3
"""
Per-test latency: from when mute criteria was satisfied (YDB test_results) to
when the test appeared in muted_ya.txt (git commit time).

Results are written incrementally to two YDB tables:
  - mute_latency            — one row per (commit × test)
  - mute_latency_affected_prs — one row per (commit × PR) that ran a muted test
                                during its latency window

Usage
-----
# Normal incremental run (picks up from last processed commit):
python3 .github/scripts/analytics/mute_latency_from_failure.py

# Force reprocess the last N days (overwrites existing rows via upsert):
python3 ... --force [--timeline-since-days 30]
"""

from __future__ import annotations

import argparse
import bisect
import datetime as dt
import os
import re
import statistics
import subprocess
import sys
from collections import defaultdict
from typing import Any, Dict, List, Optional, Sequence, Tuple

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------
_scripts_dir = os.path.dirname(os.path.abspath(__file__))
_mute_dir = os.path.normpath(os.path.join(_scripts_dir, '..', 'tests', 'mute'))
for _p in (_scripts_dir, _mute_dir):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import ydb  # noqa: E402

from mute_utils import dedicated_relative, pattern_to_re  # noqa: E402
from constants import get_mute_window_days  # noqa: E402
from ydb_wrapper import YDBWrapper  # noqa: E402


def _repo_root() -> str:
    return os.path.normpath(os.path.join(_scripts_dir, '..', '..', '..'))


_PR_NUMBER_RE = re.compile(r'(?:^|_)PR_(\d+)(?:_|$)')


def _extract_pr_number(pull: str) -> Optional[str]:
    """Extract bare PR number from pull strings like '25365126972_PR_39449_attempt_1'."""
    m = _PR_NUMBER_RE.search(pull)
    return m.group(1) if m else None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _normalize_ts(value: Any) -> Optional[dt.datetime]:
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        tz = value.tzinfo or dt.timezone.utc
        return value.replace(tzinfo=tz).astimezone(dt.timezone.utc)
    if isinstance(value, int):
        return dt.datetime.fromtimestamp(value / 1_000_000, tz=dt.timezone.utc)
    return None


def _sql_escape(s: str) -> str:
    return str(s).replace("'", "''")


def _percentile(sorted_vals: Sequence[float], q: float) -> float:
    if not sorted_vals:
        return 0.0
    if len(sorted_vals) == 1:
        return sorted_vals[0]
    k = (len(sorted_vals) - 1) * q
    f = int(k)
    c = min(f + 1, len(sorted_vals) - 1)
    return sorted_vals[f] + (sorted_vals[c] - sorted_vals[f]) * (k - f)


# ---------------------------------------------------------------------------
# YDB: when mute criteria was first satisfied (hour-precise, from test_results)
# ---------------------------------------------------------------------------
def _decode(v: Any) -> Optional[str]:
    if v is None:
        return None
    return v.decode('utf-8', errors='replace') if isinstance(v, bytes) else str(v)


def _criteria_met_in_window(
    failure_ts: List[dt.datetime],
    all_ts: List[dt.datetime],
    window_start: dt.datetime,
    window_end: dt.datetime,
    fail_threshold_high: int,
    fail_threshold_low: int,
    runs_bound: int,
) -> Optional[dt.datetime]:
    """
    Mirrors is_mute_candidate() using a fixed window [window_start, window_end].

    The mute bot runs after each CI job completes. If criteria is met at that
    point, it creates a mute PR. The triggering event is therefore the LAST
    failure in the window — the one whose job completion caused the bot to run
    and find criteria satisfied.

    Returns the timestamp of the LAST failure in the window if criteria is met,
    or None otherwise.

    Criteria:
        (fail_count >= 3 AND total_runs > 10) OR (fail_count >= 2 AND total_runs <= 10)
    """
    f_start = bisect.bisect_left(failure_ts, window_start)
    f_end   = bisect.bisect_right(failure_ts, window_end)
    a_start = bisect.bisect_left(all_ts, window_start)
    a_end   = bisect.bisect_right(all_ts, window_end)

    fails_in_window = f_end - f_start
    runs_in_window  = a_end - a_start

    if runs_in_window > runs_bound:
        threshold = fail_threshold_high
    else:
        threshold = fail_threshold_low

    if fails_in_window >= threshold:
        # The bot ran after the last failure and found criteria met.
        return failure_ts[f_end - 1]
    return None


def classify_muted_ya_lines(
    all_lines: Sequence[str],
) -> Tuple[List[Tuple[str, str]], List[str]]:
    """
    Split muted_ya lines into two groups for YDB filtering:

    exact_pairs  — [(suite_folder, test_name), ...] where NEITHER part has wildcards.
                   Can be filtered precisely as (sf = X AND tn = Y) OR ...

    wildcard_sfs — [suite_folder, ...] (deduplicated, exact) for lines where the
                   test_name has wildcards. Filtered with suite_folder IN (...);
                   Python-side matching against the full pattern happens later.

    Lines where suite_folder itself contains wildcards are skipped entirely
    (no way to express them as an efficient SQL predicate).
    """
    exact_pairs: List[Tuple[str, str]] = []
    wildcard_sfs: List[str] = []
    seen_exact: set = set()
    seen_sf: set = set()

    for line in all_lines:
        parts = line.split(' ', 1)
        if len(parts) != 2:
            continue
        sf, tn = parts[0], parts[1]

        # skip chunk pseudo-tests — same filter as in the YDB query
        if any(m in tn for m in _CHUNK_MARKERS):
            continue

        sf_wild = '*' in sf
        tn_wild = '*' in tn

        if sf_wild:
            # can't filter by this suite_folder in SQL; skip
            continue
        if tn_wild:
            if sf not in seen_sf:
                seen_sf.add(sf)
                wildcard_sfs.append(sf)
        else:
            key = (sf, tn)
            if key not in seen_exact:
                seen_exact.add(key)
                exact_pairs.append(key)

    return exact_pairs, wildcard_sfs



class _TestRuns:
    """Per-test run data collected from YDB for a given time window."""
    __slots__ = ('fail_ts', 'all_ts', 'pr_ts', 'pr_pulls')

    def __init__(
        self,
        fail_ts: List[dt.datetime],
        all_ts: List[dt.datetime],
        pr_ts: List[dt.datetime],
        pr_pulls: List[str],        # pull number parallel to pr_ts (same index)
    ) -> None:
        self.fail_ts  = fail_ts
        self.all_ts   = all_ts
        self.pr_ts    = pr_ts    # PR-check run timestamps, sorted
        self.pr_pulls = pr_pulls  # corresponding PR numbers (parallel to pr_ts)

_SF_BATCH_SIZE = 30  # suite_folders per YDB query

_CHUNK_MARKERS = ('sole chunk', 'chunk+chunk', '[chunk]', ' chunk')


def _ts_literal(t: dt.datetime) -> str:
    """Format datetime as YQL Timestamp literal."""
    return t.astimezone(dt.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def fetch_test_runs_for_window(
    ydb_wrapper: YDBWrapper,
    branch: str,
    build_type: str,
    window_start: dt.datetime,
    window_end: dt.datetime,
    exact_pairs: Optional[List[Tuple[str, str]]] = None,
    wildcard_sfs: Optional[List[str]] = None,
) -> Dict[Tuple[str, str], _TestRuns]:
    """
    Fetch run timestamps for [window_start, window_end], batching by suite_folder.

    Returns:
        (suite_folder, test_name) -> (sorted_fail_timestamps, sorted_all_timestamps)
    """
    _exact_pairs = exact_pairs or []
    _wildcard_sfs = wildcard_sfs or []
    exact_pair_set: set = set(_exact_pairs)
    wildcard_sf_set: set = set(_wildcard_sfs)
    all_sfs: List[str] = list({sf for sf, _ in _exact_pairs} | set(_wildcard_sfs))

    if not all_sfs:
        return {}

    all_runs: defaultdict  = defaultdict(list)
    fail_runs: defaultdict = defaultdict(list)
    pr_runs: defaultdict   = defaultdict(list)   # (ts, pull) pairs

    # Mirrors is_mute_candidate(): total_runs = pass + fail only; fail = 'failure' only.
    FAILURE_STATUSES = {'failure'}
    PASS_STATUSES    = {'success', 'passed'}
    _CHUNK_FILTER = """
          AND test_name NOT IN ('unittest', 'py3test', 'gtest')
          AND String::Contains(test_name, '.flake8') = FALSE
          AND NOT (
                String::Contains(test_name, 'sole chunk')
             OR String::Contains(test_name, 'chunk+chunk')
             OR String::Contains(test_name, '[chunk]')
          )"""

    table = ydb_wrapper.get_table_path('test_results')
    ts_from = _ts_literal(window_start)
    ts_to   = _ts_literal(window_end)

    batches = [all_sfs[i:i + _SF_BATCH_SIZE] for i in range(0, len(all_sfs), _SF_BATCH_SIZE)]
    for sf_batch in batches:
        quoted = ', '.join(f"'{_sql_escape(sf)}'" for sf in sf_batch)

        # Branch runs (for criteria calculation) — scheduled/postcommit jobs only, no PR-check or manual
        branch_query = f"""
        SELECT suite_folder, test_name, run_timestamp, status
        FROM `{table}`
        WHERE branch     = '{_sql_escape(branch)}'
          AND build_type = '{_sql_escape(build_type)}'
          AND job_name IN (
              'Nightly-run',
              'Regression-run',
              'Regression-run_Large',
              'Regression-run_Small_and_Medium',
              'Regression-run_compatibility',
              'Regression-whitelist-run',
              'Postcommit_relwithdebinfo',
              'Postcommit_asan'
          )
          AND (pull IS NULL OR NOT String::Contains(pull, 'manual'))
          AND run_timestamp >= Timestamp("{ts_from}")
          AND run_timestamp <= Timestamp("{ts_to}")
          AND suite_folder IN ({quoted})
          AND suite_folder IS NOT NULL
          AND test_name    IS NOT NULL
          AND Unicode::ToLower(CAST(status AS Utf8)) IN ('failure', 'success', 'passed')
          {_CHUNK_FILTER}
        """
        for row in ydb_wrapper.execute_scan_query(branch_query, query_name='mute_latency_criteria_met'):
            sf = _decode(row.get('suite_folder'))
            tn = _decode(row.get('test_name'))
            ts = _normalize_ts(row.get('run_timestamp'))
            status = (_decode(row.get('status')) or '').lower()
            if not sf or not tn or not ts:
                continue
            if sf not in wildcard_sf_set and (sf, tn) not in exact_pair_set:
                continue
            key = (sf, tn)
            if status in FAILURE_STATUSES:
                fail_runs[key].append(ts)
                all_runs[key].append(ts)
            elif status in PASS_STATUSES:
                all_runs[key].append(ts)

        # PR-check runs (for pr_runs_while_waiting / affected PRs)
        pr_query = f"""
        SELECT suite_folder, test_name, run_timestamp, pull
        FROM `{table}`
        WHERE job_name = 'PR-check'
          AND branch     = '{_sql_escape(branch)}'
          AND build_type = '{_sql_escape(build_type)}'
          AND run_timestamp >= Timestamp("{ts_from}")
          AND run_timestamp <= Timestamp("{ts_to}")
          AND suite_folder IN ({quoted})
          AND suite_folder IS NOT NULL
          AND test_name    IS NOT NULL
          {_CHUNK_FILTER}
        """
        for row in ydb_wrapper.execute_scan_query(pr_query, query_name='mute_latency_pr_runs'):
            sf   = _decode(row.get('suite_folder'))
            tn   = _decode(row.get('test_name'))
            ts   = _normalize_ts(row.get('run_timestamp'))
            pull = _decode(row.get('pull')) or ''
            if not sf or not tn or not ts:
                continue
            if sf not in wildcard_sf_set and (sf, tn) not in exact_pair_set:
                continue
            pr_runs[(sf, tn)].append((ts, pull))

    out: Dict[Tuple[str, str], _TestRuns] = {}
    for key in set(all_runs) | set(fail_runs):
        fails      = sorted(fail_runs.get(key, []))
        all_sorted = sorted(all_runs.get(key, []))
        pr_pairs   = sorted(pr_runs.get(key, []), key=lambda x: x[0])  # sort by ts
        pr_ts_list    = [ts   for ts, _ in pr_pairs]
        pr_pulls_list = [pull for _, pull in pr_pairs]
        if fails:
            out[key] = _TestRuns(fails, all_sorted, pr_ts_list, pr_pulls_list)
    return out


# ---------------------------------------------------------------------------
# Git helpers
# ---------------------------------------------------------------------------
def _git(repo: str, *args: str) -> str:
    r = subprocess.run(['git', *args], cwd=repo, capture_output=True, text=True, check=False)
    if r.returncode != 0:
        raise RuntimeError(f'git {" ".join(args)}: {r.stderr.strip()}')
    return r.stdout


def commits_touching_file(
    repo: str, rel_path: str, since_date: dt.date, branch: str = 'main'
) -> List[Tuple[str, dt.datetime]]:
    """[(sha, commit_utc), ...] oldest-first, commits touching rel_path since since_date.

    Uses origin/<branch> so the script works correctly regardless of which local
    branch is currently checked out.
    """
    git_ref = f'origin/{branch}'
    # Fall back to HEAD if origin/<branch> is not available (e.g. offline / shallow)
    try:
        _git(repo, 'rev-parse', '--verify', git_ref)
    except RuntimeError:
        git_ref = 'HEAD'
    raw = _git(
        repo,
        'rev-list', '--reverse', f'--since={since_date.isoformat()}', git_ref, '--', rel_path,
    ).strip()
    if not raw:
        return []
    out: List[Tuple[str, dt.datetime]] = []
    for sha in raw.splitlines():
        sha = sha.strip()
        if not sha:
            continue
        date_s = _git(repo, 'show', '-s', '--format=%cI', sha).strip()
        parsed = dt.datetime.fromisoformat(date_s.replace('Z', '+00:00')).astimezone(dt.timezone.utc)
        out.append((sha, parsed))
    return out


# ---------------------------------------------------------------------------
# muted_ya reading + matching
# ---------------------------------------------------------------------------
def read_muted_ya(repo: str, sha: str, rel_path: str) -> List[str]:
    try:
        content = _git(repo, 'show', f'{sha}:{rel_path}')
    except RuntimeError:
        return []
    return [
        line.strip()
        for line in content.splitlines()
        if line.strip() and not line.strip().startswith('#')
    ]


def _diff_lines(repo: str, sha: str, rel_path: str, prefix: str) -> List[str]:
    """Return lines starting with `prefix` (+/-) from the diff of sha for rel_path."""
    try:
        diff = _git(repo, 'show', '--format=', '--unified=0', sha, '--', rel_path)
    except RuntimeError:
        return []
    other = '++' if prefix == '+' else '--'
    return [
        line[1:].strip()
        for line in diff.splitlines()
        if line.startswith(prefix) and not line.startswith(other) and line[1:].strip()
        and not line[1:].strip().startswith('#')
    ]


def added_muted_ya_lines(repo: str, sha: str, rel_path: str) -> List[str]:
    """Return lines added to rel_path by this commit."""
    return _diff_lines(repo, sha, rel_path, '+')


def removed_muted_ya_lines(repo: str, sha: str, rel_path: str) -> List[str]:
    """Return lines removed from rel_path by this commit."""
    return _diff_lines(repo, sha, rel_path, '-')


def _parse_muted_ya_lines(lines: Sequence[str]) -> List[Tuple[str, str]]:
    """Parse muted_ya lines into (suite_folder, test_name) pairs, skipping chunks."""
    result = []
    for line in lines:
        parts = line.split(' ', 1)
        if len(parts) != 2:
            continue
        sf, tn = parts
        if any(m in tn for m in _CHUNK_MARKERS):
            continue
        result.append((sf, tn))
    return result


def match_muted_to_criteria(
    muted_lines: Sequence[str],
    test_runs: Dict[Tuple[str, str], _TestRuns],
    commit_time: dt.datetime,
    mute_window_days: int,
) -> Tuple[List[Dict[str, Any]], set]:
    """
    For each test in muted_lines that has run data in test_runs, compute
    criteria_met_at = last failure in [commit_time - mute_window_days, commit_time]
    where mute criteria is satisfied.

    latency_minutes = commit_time - criteria_met_at (in minutes)

    Using the commit-local window means re-muted tests and freshly-failing tests
    are both measured against the right episode, not a global historical minimum.
    """
    FAIL_THRESHOLD_HIGH = 3
    FAIL_THRESHOLD_LOW  = 2
    RUNS_BOUND          = 10
    window = dt.timedelta(days=mute_window_days)

    # build pattern list from muted_ya
    paired: List[Tuple[str, re.Pattern[str], re.Pattern[str]]] = []
    for line in muted_lines:
        parts = line.split(' ', 1)
        if len(parts) != 2:
            continue
        tn_part = parts[1]
        if any(m in tn_part for m in _CHUNK_MARKERS):
            continue
        try:
            ps = re.compile(pattern_to_re(parts[0]))
            pt = re.compile(pattern_to_re(parts[1]))
            paired.append((line, ps, pt))
        except re.error:
            pass

    window_start = commit_time - window

    rows: List[Dict[str, Any]] = []
    all_prs_in_commit: set = set()

    for (sf, tn), runs in test_runs.items():
        crit_ts = _criteria_met_in_window(
            runs.fail_ts, runs.all_ts, window_start, commit_time,
            FAIL_THRESHOLD_HIGH, FAIL_THRESHOLD_LOW, RUNS_BOUND,
        )
        if crit_ts is None:
            continue

        matched_line: Optional[str] = None
        for raw_line, ps, pt in paired:
            if ps.match(sf) and pt.match(tn):
                matched_line = raw_line
                break
        if matched_line is None:
            continue

        delta_m = (commit_time - crit_ts).total_seconds() / 60.0

        # PR-check runs for this test in [criteria_met_at, commit_time]
        pr_i = bisect.bisect_left(runs.pr_ts, crit_ts)
        pr_j = bisect.bisect_right(runs.pr_ts, commit_time)
        pr_runs_count = pr_j - pr_i
        for raw_pull in runs.pr_pulls[pr_i:pr_j]:
            pr_num = _extract_pr_number(raw_pull)
            if pr_num is not None:
                all_prs_in_commit.add(pr_num)

        rows.append({
            'full_name': f'{sf}/{tn}',
            'suite_folder': sf,
            'test_name': tn,
            'criteria_met_at': crit_ts.isoformat(),
            'commit_time': commit_time.isoformat(),
            'latency_minutes': round(delta_m, 1),
            'matched_pattern': matched_line,
            'pr_runs_while_waiting': pr_runs_count,
        })

    # unique_prs_affected is commit-level: same value on every row of this commit.
    unique_prs = len(all_prs_in_commit)
    for row in rows:
        row['unique_prs_affected'] = unique_prs

    return rows, all_prs_in_commit


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------
def print_summary(label: str, detail_rows: Sequence[Dict[str, Any]]) -> None:
    lats = sorted(r['latency_minutes'] for r in detail_rows if isinstance(r.get('latency_minutes'), (int, float)))
    if not lats:
        print(f'{label}: no matched tests')
        return
    def fmt(m: float) -> str:
        return f'{m:.0f}m' if m < 120 else f'{m/60:.1f}h'
    print(
        f'{label}: n={len(lats)}'
        f'  min={fmt(lats[0])}'
        f'  p50={fmt(statistics.median(lats))}'
        f'  p90={fmt(_percentile(lats, 0.9))}'
        f'  max={fmt(lats[-1])}'
    )


# ---------------------------------------------------------------------------
# YDB output table
# ---------------------------------------------------------------------------

def create_mute_latency_table(ydb_wrapper: YDBWrapper, table_path: str) -> None:
    print(f"> Creating table: '{table_path}'")
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS `{table_path}` (
            `branch`          Utf8 NOT NULL,
            `build_type`      Utf8 NOT NULL,
            `commit_time`     Timestamp NOT NULL,
            `commit_sha`      Utf8 NOT NULL,
            `full_name`       Utf8 NOT NULL,
            `suite_folder`    Utf8,
            `test_name`       Utf8,
            `criteria_met_at`       Timestamp,
            `latency_minutes`       Double,
            `matched_pattern`       Utf8,
            `pr_runs_while_waiting` Int32,
            `unique_prs_affected`   Int32,
            PRIMARY KEY (`branch`, `build_type`, `commit_time`, `commit_sha`, `full_name`)
        )
        PARTITION BY HASH(`branch`, `build_type`)
        WITH (STORE = COLUMN)
    """
    ydb_wrapper.create_table(table_path, create_sql)


def get_last_processed_commit_time(
    ydb_wrapper: YDBWrapper, table_path: str, branch: str, build_type: str
) -> Optional[dt.datetime]:
    """Return the MAX(commit_time) already stored for branch/build_type, or None."""
    query = f"""
        SELECT MAX(commit_time) AS last_ct
        FROM `{table_path}`
        WHERE branch = '{_sql_escape(branch)}'
          AND build_type = '{_sql_escape(build_type)}'
    """
    rows = ydb_wrapper.execute_scan_query(query, query_name='mute_latency_last_commit')
    if rows and rows[0].get('last_ct') is not None:
        return _normalize_ts(rows[0]['last_ct'])
    return None


def create_mute_latency_affected_prs_table(ydb_wrapper: YDBWrapper, table_path: str) -> None:
    print(f"> Creating table: '{table_path}'")
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS `{table_path}` (
            `branch`      Utf8 NOT NULL,
            `build_type`  Utf8 NOT NULL,
            `commit_time` Timestamp NOT NULL,
            `commit_sha`  Utf8 NOT NULL,
            `pull`        Utf8 NOT NULL,
            PRIMARY KEY (`branch`, `build_type`, `commit_time`, `commit_sha`, `pull`)
        )
        PARTITION BY HASH(`branch`, `build_type`)
        WITH (STORE = COLUMN)
    """
    ydb_wrapper.create_table(table_path, create_sql)


def create_mute_events_table(ydb_wrapper: YDBWrapper, table_path: str) -> None:
    print(f"> Creating table: '{table_path}'")
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS `{table_path}` (
            `branch`      Utf8 NOT NULL,
            `build_type`  Utf8 NOT NULL,
            `commit_time` Timestamp NOT NULL,
            `commit_sha`  Utf8 NOT NULL,
            `event`       Utf8 NOT NULL,
            `full_name`   Utf8 NOT NULL,
            `suite_folder` Utf8,
            `test_name`   Utf8,
            PRIMARY KEY (`branch`, `build_type`, `commit_time`, `commit_sha`, `event`, `full_name`)
        )
        PARTITION BY HASH(`branch`, `build_type`)
        WITH (STORE = COLUMN)
    """
    ydb_wrapper.create_table(table_path, create_sql)


def _datetime_to_us(ts: dt.datetime) -> int:
    """Convert datetime to microseconds since epoch (YDB Timestamp wire format)."""
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=dt.timezone.utc)
    return int(ts.timestamp() * 1_000_000)


def _isostr_to_us(s: Optional[str]) -> Optional[int]:
    """Parse an ISO-format string and return microseconds since epoch, or None."""
    if not s:
        return None
    try:
        ts = dt.datetime.fromisoformat(str(s))
        return _datetime_to_us(ts)
    except Exception:
        return None


def _rows_to_ydb_records(
    rows: List[Dict[str, Any]], branch: str, build_type: str
) -> List[Dict[str, Any]]:
    """Convert output rows to dicts suitable for bulk_upsert_batches."""

    return [
        {
            'branch':                branch,
            'build_type':            build_type,
            'commit_time':           _isostr_to_us(r.get('commit_time')),
            'commit_sha':            r.get('commit_sha', ''),
            'full_name':             r.get('full_name', ''),
            'suite_folder':          r.get('suite_folder'),
            'test_name':             r.get('test_name'),
            'criteria_met_at':       _isostr_to_us(r.get('criteria_met_at')),
            'latency_minutes':       r.get('latency_minutes'),
            'matched_pattern':       r.get('matched_pattern'),
            'pr_runs_while_waiting': r.get('pr_runs_while_waiting'),
            'unique_prs_affected':   r.get('unique_prs_affected'),
        }
        for r in rows
    ]


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--branch', default='main')
    parser.add_argument('--build-type', default='relwithdebinfo')
    parser.add_argument(
        '--timeline-since-days', type=int, default=None,
        help='Fallback for first run: how many days back to look if YDB has no data yet (default: 90)',
    )
    parser.add_argument(
        '--force', action='store_true', default=False,
        help='Ignore the YDB cursor and reprocess commits from --timeline-since-days (or 90 days). '
             'Existing rows are overwritten via upsert.',
    )
    args = parser.parse_args()

    repo = _repo_root()
    rel_path = dedicated_relative(args.build_type)

    _mute_win = get_mute_window_days()
    mute_delta = dt.timedelta(days=_mute_win)

    # ── Phase 1: check YDB for last processed commit (short connection) ───────
    with YDBWrapper() as w:
        if not w.check_credentials():
            print('YDB credentials check failed', file=sys.stderr)
            return 1
        mute_latency_table = w.get_table_path('mute_latency')
        create_mute_latency_table(w, mute_latency_table)
        ydb_since_dt = None if args.force else get_last_processed_commit_time(
            w, mute_latency_table, args.branch, args.build_type
        )

    if args.force:
        fallback_days = args.timeline_since_days or 90
        since_date = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=fallback_days)).date()
        print(f'Force mode: reprocessing last {fallback_days} days', file=sys.stderr)
    elif ydb_since_dt is not None:
        print(f'YDB: last processed commit_time = {ydb_since_dt.isoformat()}', file=sys.stderr)
        since_date = ydb_since_dt.date()
    else:
        print('YDB: no prior data found — will process all available commits', file=sys.stderr)
        fallback_days = args.timeline_since_days or 90
        since_date = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=fallback_days)).date()

    # ── Phase 2: git — collect commits and per-commit added lines (no YDB) ───
    commits = commits_touching_file(repo, rel_path, since_date, branch=args.branch)
    if ydb_since_dt is not None:
        commits = [(s, c) for s, c in commits if c > ydb_since_dt]
    if not commits:
        print('No new commits to process — already up to date.', file=sys.stderr)
        return 0
    print(f'{len(commits)} new commits touching {rel_path} since {since_date}', file=sys.stderr)

    # Pre-collect git data for all commits so Phase 3 has no subprocess calls.
    CommitGitData = List[Tuple[str, dt.datetime, List[str], List[str], List[Tuple[str, str]], List[str]]]
    commit_git_data: CommitGitData = []
    for i, (sha, ct) in enumerate(commits, 1):
        added_lines   = added_muted_ya_lines(repo, sha, rel_path)
        removed_lines = removed_muted_ya_lines(repo, sha, rel_path)
        if not added_lines and not removed_lines:
            print(f'  [{i}/{len(commits)}] {sha[:12]} {ct.date()} — no changes, skip', file=sys.stderr)
            continue
        exact_pairs, wildcard_sfs = classify_muted_ya_lines(added_lines)
        print(
            f'  [{i}/{len(commits)}] {sha[:12]} {ct.date()} '
            f'+{len(added_lines)} -{len(removed_lines)} lines '
            f'({len(exact_pairs)} exact, {len(wildcard_sfs)} wc_sfs)',
            file=sys.stderr,
        )
        commit_git_data.append((sha, ct, added_lines, removed_lines, exact_pairs, wildcard_sfs))

    if not commit_git_data:
        print('No commits with mute changes — nothing to upload.', file=sys.stderr)
        return 0

    # ── Phase 3: YDB queries + upload (no subprocess calls) ──────────────────
    all_rows: List[Dict[str, Any]] = []
    all_pr_rows: List[Dict[str, Any]] = []
    all_event_rows: List[Dict[str, Any]] = []

    with YDBWrapper() as w:
        affected_prs_table = w.get_table_path('mute_latency_affected_prs')
        create_mute_latency_affected_prs_table(w, affected_prs_table)
        mute_events_table = w.get_table_path('mute_events')
        create_mute_events_table(w, mute_events_table)

        for sha, ct, added_lines, removed_lines, exact_pairs, wildcard_sfs in commit_git_data:
            # mute_events: record added and removed tests
            for event, lines in (('added', added_lines), ('removed', removed_lines)):
                for sf, tn in _parse_muted_ya_lines(lines):
                    all_event_rows.append({
                        'branch':       args.branch,
                        'build_type':   args.build_type,
                        'commit_time':  ct,
                        'commit_sha':   sha,
                        'event':        event,
                        'full_name':    f'{sf}/{tn}',
                        'suite_folder': sf,
                        'test_name':    tn,
                    })

            # mute_latency: only for commits with additions
            if not exact_pairs and not wildcard_sfs:
                continue
            window_start = ct - mute_delta
            test_runs = fetch_test_runs_for_window(
                w, args.branch, args.build_type,
                window_start, ct,
                exact_pairs=exact_pairs,
                wildcard_sfs=wildcard_sfs,
            )
            rows, affected_prs = match_muted_to_criteria(added_lines, test_runs, ct, _mute_win)
            for row in rows:
                row['commit_sha'] = sha
                all_rows.append(row)
            for pull in affected_prs:
                all_pr_rows.append({
                    'branch':      args.branch,
                    'build_type':  args.build_type,
                    'commit_time': ct,
                    'commit_sha':  sha,
                    'pull':        pull,
                })

        all_rows.sort(key=lambda r: (r['commit_time'], r['full_name']))

        if all_rows:
            ydb_records = _rows_to_ydb_records(all_rows, args.branch, args.build_type)
            column_types = (
                ydb.BulkUpsertColumns()
                .add_column('branch',               ydb.OptionalType(ydb.PrimitiveType.Utf8))
                .add_column('build_type',           ydb.OptionalType(ydb.PrimitiveType.Utf8))
                .add_column('commit_time',          ydb.OptionalType(ydb.PrimitiveType.Timestamp))
                .add_column('commit_sha',           ydb.OptionalType(ydb.PrimitiveType.Utf8))
                .add_column('full_name',            ydb.OptionalType(ydb.PrimitiveType.Utf8))
                .add_column('suite_folder',         ydb.OptionalType(ydb.PrimitiveType.Utf8))
                .add_column('test_name',            ydb.OptionalType(ydb.PrimitiveType.Utf8))
                .add_column('criteria_met_at',      ydb.OptionalType(ydb.PrimitiveType.Timestamp))
                .add_column('latency_minutes',      ydb.OptionalType(ydb.PrimitiveType.Double))
                .add_column('matched_pattern',      ydb.OptionalType(ydb.PrimitiveType.Utf8))
                .add_column('pr_runs_while_waiting',ydb.OptionalType(ydb.PrimitiveType.Int32))
                .add_column('unique_prs_affected',  ydb.OptionalType(ydb.PrimitiveType.Int32))
            )
            w.bulk_upsert_batches(mute_latency_table, ydb_records, column_types, query_name='mute_latency_upload')
            print_summary('Overall', all_rows)
            print(f'Uploaded {len(ydb_records)} rows → YDB {mute_latency_table}', file=sys.stderr)
        else:
            print('No new rows to upload.', file=sys.stderr)

        if all_pr_rows:
            pr_records = [
                {**r, 'commit_time': _datetime_to_us(r['commit_time'])}
                for r in all_pr_rows
            ]
            pr_column_types = (
                ydb.BulkUpsertColumns()
                .add_column('branch',      ydb.OptionalType(ydb.PrimitiveType.Utf8))
                .add_column('build_type',  ydb.OptionalType(ydb.PrimitiveType.Utf8))
                .add_column('commit_time', ydb.OptionalType(ydb.PrimitiveType.Timestamp))
                .add_column('commit_sha',  ydb.OptionalType(ydb.PrimitiveType.Utf8))
                .add_column('pull',        ydb.OptionalType(ydb.PrimitiveType.Utf8))
            )
            w.bulk_upsert_batches(
                affected_prs_table, pr_records, pr_column_types,
                query_name='mute_latency_affected_prs_upload',
            )
            print(f'Uploaded {len(pr_records)} rows → YDB {affected_prs_table}', file=sys.stderr)

        if all_event_rows:
            event_records = [
                {**r, 'commit_time': _datetime_to_us(r['commit_time'])}
                for r in all_event_rows
            ]
            event_column_types = (
                ydb.BulkUpsertColumns()
                .add_column('branch',       ydb.OptionalType(ydb.PrimitiveType.Utf8))
                .add_column('build_type',   ydb.OptionalType(ydb.PrimitiveType.Utf8))
                .add_column('commit_time',  ydb.OptionalType(ydb.PrimitiveType.Timestamp))
                .add_column('commit_sha',   ydb.OptionalType(ydb.PrimitiveType.Utf8))
                .add_column('event',        ydb.OptionalType(ydb.PrimitiveType.Utf8))
                .add_column('full_name',    ydb.OptionalType(ydb.PrimitiveType.Utf8))
                .add_column('suite_folder', ydb.OptionalType(ydb.PrimitiveType.Utf8))
                .add_column('test_name',    ydb.OptionalType(ydb.PrimitiveType.Utf8))
            )
            w.bulk_upsert_batches(
                mute_events_table, event_records, event_column_types,
                query_name='mute_events_upload',
            )
            print(f'Uploaded {len(event_records)} rows → YDB {mute_events_table}', file=sys.stderr)

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
