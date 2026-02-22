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
