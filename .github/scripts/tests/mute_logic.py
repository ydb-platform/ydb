"""
Pure mute logic — no YDB, no external deps. Used by create_new_muted_ya and tests.
"""
import datetime
import logging
import re
from collections import defaultdict

# Configure logging for library use
_log = logging.getLogger(__name__)


def _matches_error_filter(error_type, error_filter):
    """Check if error_type matches error_filter."""
    if not error_filter:
        return True
    err = (error_type or '').upper()
    includes = [str(x).strip().upper() for x in error_filter if not str(x).strip().startswith('!')]
    excludes = [str(x).strip().upper()[1:] for x in error_filter if str(x).strip().upper().startswith('!')]
    if excludes and any(ex and ex in err for ex in excludes):
        return False
    if includes and not any(inc and inc in err for inc in includes):
        return False
    return True


def is_mute_candidate(test, aggregated_data, params=None):
    p = params or {}
    min_fail_high = p.get('min_failures_high', 3)
    min_fail_low = p.get('min_failures_low', 2)
    min_runs_threshold = p.get('min_runs_threshold', 10)
    error_filter = p.get('error_filter')

    test_data = next((a for a in aggregated_data if a['full_name'] == test.get('full_name')), None)
    if not test_data:
        return False

    test['pass_count'] = test_data['pass_count']
    test['fail_count'] = test_data['fail_count']
    test['period_days'] = test_data.get('period_days')
    test['is_muted'] = test_data.get('is_muted', False)

    if test_data.get('is_muted', False):
        return False
    if error_filter and 'error_type' in test_data:
        if not _matches_error_filter(test_data.get('error_type'), error_filter):
            return False

    total_runs = test_data['pass_count'] + test_data['fail_count']
    return (
        (test_data['fail_count'] >= min_fail_high and total_runs > min_runs_threshold)
        or (test_data['fail_count'] >= min_fail_low and total_runs <= min_runs_threshold)
    )


def is_unmute_candidate(test, aggregated_data, params=None):
    p = params or {}
    min_runs = p.get('min_runs', 4)
    max_fails = p.get('max_fails', 0)
    test_data = next((a for a in aggregated_data if a['full_name'] == test.get('full_name')), None)
    if not test_data:
        return False
    test['pass_count'] = test_data['pass_count']
    test['fail_count'] = test_data['fail_count']
    test['mute_count'] = test_data['mute_count']
    test['period_days'] = test_data.get('period_days')
    test['is_muted'] = test_data.get('is_muted', False)
    total_runs = test_data['pass_count'] + test_data['fail_count'] + test_data['mute_count']
    total_fails = test_data['fail_count'] + test_data['mute_count']
    return total_runs >= min_runs and total_fails <= max_fails


def is_delete_candidate(test, aggregated_data, params=None):
    test_data = next((a for a in aggregated_data if a['full_name'] == test.get('full_name')), None)
    if not test_data:
        return False
    test['pass_count'] = test_data['pass_count']
    test['fail_count'] = test_data['fail_count']
    test['mute_count'] = test_data['mute_count']
    test['skip_count'] = test_data['skip_count']
    test['period_days'] = test_data.get('period_days')
    test['is_muted'] = test_data.get('is_muted', False)
    total_runs = test_data['pass_count'] + test_data['fail_count'] + test_data['mute_count'] + test_data['skip_count']
    return total_runs == 0


def is_chunk_test(test):
    if 'is_test_chunk' in test:
        return bool(test['is_test_chunk'])
    return "chunk" in test.get('test_name', '').lower()


def create_test_string(test, use_wildcards=False):
    testsuite = test.get('suite_folder')
    testcase = test.get('test_name')
    test_string = f"{testsuite} {testcase}"
    if use_wildcards:
        test_string = re.sub(r'\d+/(\d+)\]', r'*/*]', test_string)
    return test_string


def create_debug_string(test, success_rate=None, period_days=None, date_window=None):
    if success_rate is None:
        runs = test.get('pass_count', 0) + test.get('fail_count', 0)
        success_rate = round((test.get('pass_count', 0) / runs * 100), 1) if runs > 0 else 0
    testsuite = test.get('suite_folder')
    testcase = test.get('test_name')
    runs = test.get('pass_count', 0) + test.get('fail_count', 0) + test.get('mute_count', 0)
    debug_string = f"{testsuite} {testcase} # owner {test.get('owner', 'N/A')} success_rate {success_rate}%"
    if period_days:
        debug_string += f" (last {period_days} days)"
    if date_window:
        debug_string += f" [{date_window}]"
    state = test.get('state', 'N/A')
    if is_chunk_test(test):
        state = "(chunk)"
    is_muted = test.get('is_muted', False)
    mute_state = "muted" if is_muted else "not muted"
    debug_string += f", p-{test.get('pass_count')}, f-{test.get('fail_count')},m-{test.get('mute_count')}, s-{test.get('skip_count')}, runs-{runs}, mute state: {mute_state}, test state {state}"
    return debug_string


def _to_days(date_value):
    base_date = datetime.date(1970, 1, 1)
    if date_value is None:
        return -1
    if isinstance(date_value, datetime.date):
        return (date_value - base_date).days
    return date_value


def aggregate_test_data(all_data, period_days):
    today = datetime.date.today()
    base_date = datetime.date(1970, 1, 1)
    start_date = today - datetime.timedelta(days=period_days - 1)
    start_days = (start_date - base_date).days
    aggregated = {}
    sorted_data = sorted(all_data, key=lambda t: (_to_days(t.get('date_window')), t.get('full_name') or ''))
    for test in sorted_data:
        if _to_days(test.get('date_window', 0)) >= start_days:
            full_name = test.get('full_name')
            if full_name not in aggregated:
                aggregated[full_name] = {
                    'test_name': test.get('test_name'),
                    'suite_folder': test.get('suite_folder'),
                    'full_name': full_name,
                    'build_type': test.get('build_type'),
                    'branch': test.get('branch'),
                    'pass_count': 0,
                    'fail_count': 0,
                    'mute_count': 0,
                    'skip_count': 0,
                    'owner': test.get('owner'),
                    'is_muted': test.get('is_muted'),
                    'is_muted_date': test.get('date_window'),
                    'state': test.get('state'),
                    'state_history': [test.get('state')],
                    'state_dates': [test.get('date_window')],
                    'days_in_state': test.get('days_in_state'),
                    'date_window': test.get('date_window'),
                    'period_days': period_days,
                    'is_test_chunk': test.get('is_test_chunk', 0),
                }
            else:
                current = test.get('state')
                current_date = test.get('date_window')
                if current and (not aggregated[full_name]['state_history'] or aggregated[full_name]['state_history'][-1] != current):
                    aggregated[full_name]['state_history'].append(current)
                    aggregated[full_name]['state_dates'].append(current_date)
                if _to_days(test.get('date_window', 0)) > _to_days(aggregated[full_name].get('is_muted_date', 0)):
                    aggregated[full_name]['is_muted'] = test.get('is_muted')
                    aggregated[full_name]['is_muted_date'] = test.get('date_window')
            aggregated[full_name]['pass_count'] += test.get('pass_count', 0)
            aggregated[full_name]['fail_count'] += test.get('fail_count', 0)
            aggregated[full_name]['mute_count'] += test.get('mute_count', 0)
            aggregated[full_name]['skip_count'] += test.get('skip_count', 0)
    date_window_range = f"{start_date.strftime('%Y-%m-%d')}:{today.strftime('%Y-%m-%d')}"
    for test_data in aggregated.values():
        test_data['date_window'] = date_window_range
        total_runs = test_data['pass_count'] + test_data['fail_count'] + test_data['mute_count'] + test_data['skip_count']
        test_data['success_rate'] = round((test_data['pass_count'] / total_runs) * 100, 1) if total_runs > 0 else 0.0
        test_data['summary'] = f"p-{test_data['pass_count']}, f-{test_data['fail_count']}, m-{test_data['mute_count']}, s-{test_data['skip_count']}, total-{total_runs}"
        if 'state_history' in test_data and len(test_data['state_history']) > 1:
            state_with_dates = []
            for state, date in zip(test_data['state_history'], test_data['state_dates']):
                if date:
                    if isinstance(date, int):
                        date_obj = base_date + datetime.timedelta(days=date)
                    else:
                        date_obj = date
                    state_with_dates.append(f"{state}({date_obj.strftime('%Y-%m-%d')})")
                else:
                    state_with_dates.append(state)
            test_data['state'] = '->'.join(state_with_dates)
    return list(aggregated.values())


def get_quarantine_graduation(quarantine_tests, aggregated_1day, params=None):
    p = params or {}
    min_runs = p.get('min_runs', 4)
    min_passes = p.get('min_passes', 1)
    graduated = set()
    for test_line in quarantine_tests:
        parts = test_line.split(" ", maxsplit=1)
        if len(parts) != 2:
            continue
        suite, testcase = parts
        full_name = f"{suite}/{testcase}"
        agg = next((a for a in aggregated_1day if a.get('full_name') == full_name), None)
        if not agg:
            continue
        total_runs = agg.get('pass_count', 0) + agg.get('fail_count', 0) + agg.get('mute_count', 0)
        pass_count = agg.get('pass_count', 0)
        if total_runs >= min_runs and pass_count >= min_passes:
            graduated.add(test_line)
    return graduated
