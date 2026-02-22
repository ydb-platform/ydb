"""Pure PR-check pattern logic — no YDB deps. Used by evaluate_pr_check_rules and tests."""
import datetime
from collections import defaultdict


def _parse_date(d):
    if hasattr(d, 'date') and callable(getattr(d, 'date', None)):
        return d.date()
    if isinstance(d, datetime.date):
        return d
    if isinstance(d, str):
        return datetime.datetime.strptime(d[:10], '%Y-%m-%d').date()
    return d


def pattern_floating_across_days(runs, params):
    """Timeout failures floating across different tests in a suite over the window."""
    min_total = params.get('min_total_fails', 3)
    max_per_test = params.get('max_per_test', 2)
    by_suite = defaultdict(lambda: defaultdict(lambda: {'timeout': 0, 'other': 0}))
    for r in runs:
        status = (r.get('status') or '').lower()
        if status not in ('failure', 'error'):
            continue
        err = (r.get('error_type') or '').upper()
        is_timeout = 'TIMEOUT' in err or err == 'TIMEOUT'
        suite = r.get('suite_folder') or ''
        full_name = r.get('full_name') or ''
        if is_timeout:
            by_suite[suite][full_name]['timeout'] += 1
        else:
            by_suite[suite][full_name]['other'] += 1
    matches = []
    for suite, tests in by_suite.items():
        total_timeouts = sum(t['timeout'] for t in tests.values())
        max_per = max(t['timeout'] for t in tests.values()) if tests else 0
        if total_timeouts >= min_total and max_per <= max_per_test:
            matches.append({
                'pattern': 'floating_across_days',
                'suite_folder': suite,
                'total_timeouts': total_timeouts,
                'max_per_test': max_per,
                'tests_count': len(tests),
            })
    return matches


def pattern_retry_recovered(runs, params):
    by_job_test = defaultdict(list)
    for r in runs:
        jid = r.get('job_id')
        fn = r.get('full_name') or ''
        if jid is None or not fn:
            continue
        ts = r.get('run_timestamp')
        status = (r.get('status') or '').lower()
        by_job_test[(jid, fn)].append((ts, status))
    matches = []
    for (jid, fn), events in by_job_test.items():
        events.sort(key=lambda x: x[0])
        statuses = [s for _, s in events]
        if len(statuses) >= 2:
            first = statuses[0]
            rest = statuses[1:]
            if first in ('failure', 'error') and any(s in ('passed', 'ok') for s in rest):
                matches.append({'pattern': 'retry_recovered', 'job_id': jid, 'full_name': fn, 'events': len(events)})
    return matches


def pattern_muted_test_different_error(runs, muted_tests, params):
    matches = []
    for r in runs:
        status = (r.get('status') or '').lower()
        if status not in ('failure', 'error'):
            continue
        suite = r.get('suite_folder') or ''
        name = r.get('test_name') or ''
        if (suite, name) not in muted_tests:
            continue
        err = (r.get('error_type') or '').strip()
        if err:
            matches.append({
                'pattern': 'muted_test_different_error',
                'full_name': r.get('full_name'),
                'error_type': err,
                'suite_folder': suite,
                'test_name': name,
            })
    return matches


def _median(values):
    """Median of non-empty list of numbers."""
    if not values:
        return None
    sorted_vals = sorted(v for v in values if v is not None and v > 0)
    if not sorted_vals:
        return None
    n = len(sorted_vals)
    return sorted_vals[n // 2] if n % 2 else (sorted_vals[n // 2 - 1] + sorted_vals[n // 2]) / 2


def pattern_duration_increased(runs, params):
    """
    Detect tests whose duration has increased significantly.
    Compares median duration in recent days vs baseline (older days).
    """
    window_days = params.get('window_days', 7)
    baseline_days = params.get('baseline_days', window_days - 1)
    recent_days = params.get('recent_days', 1)
    min_baseline_runs = params.get('min_baseline_runs', 3)
    min_recent_runs = params.get('min_recent_runs', 2)
    growth_factor = params.get('growth_factor', 1.5)

    by_test = defaultdict(lambda: [])
    for r in runs:
        full_name = r.get('full_name') or ''
        duration = r.get('duration')
        if not full_name or duration is None:
            continue
        ts = r.get('run_timestamp')
        if ts is None:
            continue
        d = _parse_date(ts)
        by_test[full_name].append((d, float(duration)))

    today = max((d for runs_list in by_test.values() for d, _ in runs_list), default=None)
    if today is None:
        return []

    baseline_end = today - datetime.timedelta(days=recent_days)
    baseline_start = baseline_end - datetime.timedelta(days=baseline_days)
    recent_start = baseline_end

    matches = []
    for full_name, events in by_test.items():
        baseline_durations = [dur for d, dur in events if baseline_start <= d < baseline_end]
        recent_durations = [dur for d, dur in events if d >= recent_start and dur > 0]

        if len(baseline_durations) < min_baseline_runs or len(recent_durations) < min_recent_runs:
            continue

        median_baseline = _median(baseline_durations)
        median_recent = _median(recent_durations)
        if median_baseline is None or median_recent is None or median_baseline <= 0:
            continue

        if median_recent >= median_baseline * growth_factor:
            matches.append({
                'pattern': 'duration_increased',
                'full_name': full_name,
                'baseline_median': median_baseline,
                'recent_median': median_recent,
                'growth_ratio': median_recent / median_baseline,
                'baseline_runs': len(baseline_durations),
                'recent_runs': len(recent_durations),
            })
    return matches
