#!/usr/bin/env python3
import argparse
import datetime
import json
import os
import re
import ydb
import logging
import sys
from collections import defaultdict

# Runnable as ``python3 .github/scripts/tests/mute/create_new_muted_ya.py``: expose package ``mute``.
_mutedir = os.path.dirname(os.path.abspath(__file__))
_tests_dir = os.path.dirname(_mutedir)
_scripts_dir = os.path.dirname(_tests_dir)
for _p in (_tests_dir, _scripts_dir, os.path.join(_scripts_dir, 'analytics')):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from mute.mute_check import YaMuteCheck
from mute.update_mute_issues import (
    ORG_NAME,
    PROJECT_ID,
    close_unmuted_issues,
    create_and_add_issue_to_project,
    generate_github_issue_title_and_body,
    get_issues_and_tests_from_project,
    get_muted_tests_from_issues,
    map_tests_to_manual_fast_unmute_issue_url,
)
from mute.constants import (
    get_delete_window_days,
    get_manual_unmute_min_runs,
    get_manual_unmute_window_days,
    get_mute_window_days,
    get_unmute_window_days,
)
from mute.naming import mute_file_line_to_tests_monitor_full_name
from mute.mute_utils import dedicated_relative
from ydb_wrapper import YDBWrapper
from github_issue_utils import DEFAULT_BUILD_TYPE, canonical_team_slug, make_profile_id

# Configure logging — root INFO so ydb/grpc don't spam DEBUG (channel options, etc.).
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
for _noisy in ('grpc', 'grpc._cython.cygrpc', 'ydb'):
    logging.getLogger(_noisy).setLevel(logging.WARNING)

dir = os.path.dirname(__file__)
repo_path = os.path.normpath(os.path.join(dir, '..', '..', '..', '..')) + os.sep

_DIGEST_NOTIFICATION_CONFIG = os.path.normpath(
    os.path.join(dir, '..', '..', '..', 'config', 'mute_issue_and_digest_config.json')
)

def load_manual_unmute_config():
    """Manual fast-unmute window — required keys in ``mute_config.json`` via ``mute.constants``."""
    return get_manual_unmute_window_days(), get_manual_unmute_min_runs()


def resolve_muted_ya_path(explicit_path: str | None, build_type: str) -> str:
    if explicit_path and str(explicit_path).strip():
        return str(explicit_path).strip()
    return dedicated_relative(build_type)


def tests_monitor_query_days_window():
    """How many calendar days of ``tests_monitor`` history we must load for mute/unmute/delete/fast-unmute."""
    return max(
        get_mute_window_days(),
        get_unmute_window_days(),
        get_delete_window_days(),
        get_manual_unmute_window_days(),
    )


def grace_started_at_to_utc_date(value):
    """Normalize ``grace_started_at`` from scan_query (datetime, date, or int microseconds)."""
    if value is None:
        return None
    if isinstance(value, datetime.datetime):
        return value.astimezone(datetime.timezone.utc).date()
    if isinstance(value, datetime.date):
        return value
    if isinstance(value, int):
        return datetime.datetime.fromtimestamp(
            value / 1_000_000, tz=datetime.timezone.utc
        ).date()
    return None


def merge_mute_aggregate_with_fast_unmute_grace(
    all_data,
    aggregated_for_mute_default,
    grace_map,
    manual_window_days,
    mute_window_days,
):
    """Rebuild mute aggregation list so tests in post–fast-unmute grace use a ladder window.

    Effective window = ``min(mute_window_days, manual_window_days + days_since_grace_started)`` calendar days.
    """
    if not grace_map:
        return aggregated_for_mute_default

    today = datetime.datetime.now(datetime.timezone.utc).date()
    # Which ladder window lengths (eff) actually occur for grace rows. We aggregate only
    # those periods — not every integer from manual_window_days..mute_window_days — so we
    # skip redundant aggregate_test_data calls when few distinct eff values appear.
    needed_effs = set()
    for meta in grace_map.values():
        gs_date = grace_started_at_to_utc_date(meta.get('grace_started_at'))
        if gs_date is None:
            continue
        days_since = max(0, (today - gs_date).days)
        needed_effs.add(min(mute_window_days, manual_window_days + days_since))

    # Per eff: full_name -> aggregated row. Lets the loop below do dict lookups instead of
    # rebuilding {full_name: row} from the agg list for every test (same result, less work).
    maps_by_eff = {}
    for d in sorted(needed_effs):
        agg_list = aggregate_test_data(all_data, d)
        maps_by_eff[d] = {t['full_name']: t for t in agg_list}

    by_name = {t['full_name']: t for t in aggregated_for_mute_default}
    merged = []
    for fn, test in by_name.items():
        meta = grace_map.get(fn)
        if meta:
            gs_date = grace_started_at_to_utc_date(meta.get('grace_started_at'))
            if gs_date is not None:
                days_since = max(0, (today - gs_date).days)
                eff = min(mute_window_days, manual_window_days + days_since)
                alt_map = maps_by_eff.get(eff)
                if alt_map is not None:
                    test = alt_map.get(fn, test)
        merged.append(test)
    return merged


def load_fast_unmute_grace_map(ydb_wrapper, branch, build_type):
    """Rows in ``fast_unmute_grace``: widening mute threshold after a test left fast-unmute."""
    try:
        table_path = ydb_wrapper.get_table_path('fast_unmute_grace')
    except KeyError:
        logging.info('fast_unmute_grace not registered in ydb_qa_config — ladder disabled')
        return {}

    branch_esc = str(branch).replace("'", "''")
    bt_esc = str(build_type).replace("'", "''")
    query = f"""
    SELECT full_name, grace_started_at
    FROM `{table_path}`
    WHERE branch = '{branch_esc}'
        AND build_type = '{bt_esc}'
    """
    try:
        rows = ydb_wrapper.execute_scan_query(query, query_name='fast_unmute_grace_load')
    except Exception as exc:
        logging.warning('Failed to load fast_unmute_grace: %s', exc)
        return {}

    out = {}
    for row in rows:
        fn = row.get('full_name')
        if fn:
            out[fn] = row
    return out


def delete_fast_unmute_grace_rows(ydb_wrapper, branch, build_type, test_strings):
    """Remove grace rows when tests are mute candidates again (wildcards skipped)."""
    if not test_strings:
        return
    try:
        table_path = ydb_wrapper.get_table_path('fast_unmute_grace')
    except KeyError:
        return

    for line in test_strings:
        if '*' in line or '?' in line:
            continue
        full_name = mute_file_line_to_tests_monitor_full_name(line)
        query = f"""
        DECLARE $full_name AS Utf8;
        DECLARE $branch AS Utf8;
        DECLARE $build_type AS Utf8;
        DELETE FROM `{table_path}`
        WHERE full_name = $full_name
            AND branch = $branch
            AND build_type = $build_type;
        """
        try:
            ydb_wrapper.execute_dml(
                query,
                {'$full_name': full_name, '$branch': branch, '$build_type': build_type},
                query_name='fast_unmute_grace_delete_on_remute',
            )
        except Exception as exc:
            logging.warning(
                'Failed to delete grace row for mute line %r (monitor key %r): %s',
                line,
                full_name,
                exc,
            )


def load_manual_unmute_full_names(ydb_wrapper, branch, build_type):
    """Return the set of full_name registered for manual fast-unmute on this (branch, build_type)."""
    try:
        table_path = ydb_wrapper.get_table_path('fast_unmute_active')
    except KeyError:
        logging.info('fast_unmute_active not registered in ydb_qa_config — manual fast-unmute disabled')
        return set()

    branch_escaped = str(branch).replace("'", "''")
    build_type_escaped = str(build_type).replace("'", "''")
    query = f"""
    SELECT full_name
    FROM `{table_path}`
    WHERE branch = '{branch_escaped}'
        AND build_type = '{build_type_escaped}'
    """
    try:
        rows = ydb_wrapper.execute_scan_query(query, query_name='manual_unmute_load_full_names')
    except Exception as exc:
        logging.warning('Failed to load fast_unmute_active: %s — manual fast-unmute disabled', exc)
        return set()
    return {row['full_name'] for row in rows if row.get('full_name')}


def is_chunk_test(test):
    # First, check the is_test_chunk field if it exists.
    if 'is_test_chunk' in test:
        return bool(test['is_test_chunk'])
    # Fallback to legacy name-based logic.
    return "chunk" in test.get('test_name', '').lower()

def get_wildcard_unmute_candidates(aggregated_for_unmute, mute_check, is_unmute_candidate):
    """
    Return a list of (pattern, debug_string) for wildcard patterns where all chunks pass the unmute filter.
    If at least one chunk does not pass, the pattern is not included in unmute candidates.
    The debug string is taken from any passing chunk (usually the first one).
    This guarantees that if a pattern is unmuted, all its chunks are unmuted as well, and vice versa.
    """
    wildcard_groups = {}
    for test in aggregated_for_unmute:
        if mute_check and mute_check(test.get('suite_folder'), test.get('test_name')):
            wildcard_pattern = create_test_string(test, use_wildcards=True)
            if wildcard_pattern not in wildcard_groups:
                wildcard_groups[wildcard_pattern] = []
            wildcard_groups[wildcard_pattern].append(test)

    result = []
    for pattern, chunks in wildcard_groups.items():
        passed_chunks = [chunk for chunk in chunks if is_unmute_candidate(chunk)]
        if len(passed_chunks) == len(chunks) and chunks:
            debug = create_debug_string(
                passed_chunks[0],
                period_days=passed_chunks[0].get('period_days'),
                date_window=passed_chunks[0].get('date_window')
            )
            result.append((pattern, debug))
    return result


def get_wildcard_delete_candidates(aggregated_for_delete, mute_check, is_delete_candidate):
    """
    Return a list of (pattern, debug_string) for wildcard patterns where all chunks pass the delete filter.
    If at least one chunk does not pass, the pattern is not included in delete candidates.
    The debug string is taken from any passing chunk (usually the first one).
    This guarantees that if mute is removed by pattern, it is removed for all chunks, and vice versa.
    """
    wildcard_groups = {}
    for test in aggregated_for_delete:
        if mute_check and mute_check(test.get('suite_folder'), test.get('test_name')):
            wildcard_pattern = create_test_string(test, use_wildcards=True)
            if wildcard_pattern not in wildcard_groups:
                wildcard_groups[wildcard_pattern] = []
            wildcard_groups[wildcard_pattern].append(test)

    result = []
    for pattern, chunks in wildcard_groups.items():
        passed_chunks = [chunk for chunk in chunks if is_delete_candidate(chunk)]
        if len(passed_chunks) == len(chunks) and chunks:
            debug = create_debug_string(
                passed_chunks[0],
                period_days=passed_chunks[0].get('period_days'),
                date_window=passed_chunks[0].get('date_window')
            )
            result.append((pattern, debug))
    return result


def execute_query(branch='main', build_type=DEFAULT_BUILD_TYPE, days_window=None, ydb_wrapper=None):
    if days_window is None:
        days_window = tests_monitor_query_days_window()
    logging.info(f"Executing query for branch='{branch}', build_type='{build_type}', days_window={days_window}")
    
    def _run(w):
        tests_monitor_table = w.get_table_path("tests_monitor")
        
        query_string = f'''
    SELECT 
        test_name, 
        suite_folder, 
        full_name, 
        build_type, 
        branch, 
        date_window,
        pass_count, 
        fail_count, 
        mute_count, 
        skip_count, 
        success_rate, 
        owner, 
        is_muted, 
        state, 
        days_in_state,
        is_test_chunk
    FROM `{tests_monitor_table}`
    WHERE date_window >= CurrentUtcDate() - {days_window}*Interval("P1D")
        AND branch = '{branch}' 
        AND build_type = '{build_type}'
    '''
        
        logging.info(f"SQL Query:\n{query_string}")
        
        results = w.execute_scan_query(query_string, query_name=f"get_tests_monitor_data_{branch}")
        
        suite_test_names = {'unittest', 'py3test', 'gtest'}
        filtered_results = [
            result for result in results
            if result.get('test_name') and result.get('test_name') not in suite_test_names
        ]
        
        logging.info(f"Query completed. Total rows: {len(results)}, after filtering: {len(filtered_results)}")
        return filtered_results

    try:
        if ydb_wrapper:
            return _run(ydb_wrapper)
        with YDBWrapper() as w:
            if not w.check_credentials():
                return []
            return _run(w)

    except Exception as e:
        logging.error(f"Error executing query: {e}")
        logging.error(f"Query parameters: branch='{branch}', build_type='{build_type}', days_window={days_window}")
        raise


def add_lines_to_file(file_path, lines_to_add):
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'w') as f:
            f.writelines(lines_to_add)
        logging.info(f"Lines added to {file_path}")
    except Exception as e:
        logging.error(f"Error adding lines to {file_path}: {e}")

def aggregate_test_data(all_data, period_days):
    """Universal helper to aggregate test data for a given period."""
    today = datetime.date.today()
    base_date = datetime.date(1970, 1, 1)
    start_date = today - datetime.timedelta(days=period_days-1)
    start_days = (start_date - base_date).days
    
    # Helper function to convert date to days if needed
    def to_days(date_value):
        if date_value is None:
            return -1
        if isinstance(date_value, datetime.date):
            return (date_value - base_date).days
        return date_value
    
    logging.info(f"Starting aggregation for {period_days} days period...")
    logging.info(f"Processing {len(all_data)} test records...")
    
    # Aggregate by full_name.
    aggregated = {}
    processed_count = 0
    total_count = len(all_data)
    
    # Sort records by date so state history is built chronologically.
    sorted_data = sorted(
        all_data,
        key=lambda test: (
            to_days(test.get('date_window')),
            test.get('full_name') or ''
        )
    )
    
    for test in sorted_data:
        processed_count += 1
        # Report progress every 10k records or every 10%.
        if processed_count % 10000 == 0 or processed_count % max(1, total_count // 10) == 0:
            progress_percent = (processed_count / total_count) * 100
            logging.info(f"Aggregation progress: {processed_count}/{total_count} ({progress_percent:.1f}%)")
        
        if to_days(test.get('date_window', 0)) >= start_days:
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
                    'owner_date': test.get('date_window'),
                    'is_muted': test.get('is_muted'),
                    'is_muted_date': test.get('date_window'),  # Store the date used for is_muted.
                    'state': test.get('state'),
                    'state_history': [test.get('state')],  # Initialize state history.
                    'state_dates': [test.get('date_window')],  # Track dates for state history.
                    'days_in_state': test.get('days_in_state'),
                    'date_window': test.get('date_window'),
                    'period_days': period_days,  # Keep period information for debug output.
                    'is_test_chunk': test.get('is_test_chunk', 0),
                }
            else:
                # Append state to history if it differs from the previous state.
                current_state = test.get('state')
                current_date = test.get('date_window')
                if current_state and (not aggregated[full_name]['state_history'] or aggregated[full_name]['state_history'][-1] != current_state):
                    aggregated[full_name]['state_history'].append(current_state)
                    aggregated[full_name]['state_dates'].append(current_date)
                
                # Update is_muted when the current row has a newer date.
                if to_days(test.get('date_window', 0)) > to_days(aggregated[full_name].get('is_muted_date', 0)):
                    aggregated[full_name]['is_muted'] = test.get('is_muted')
                    aggregated[full_name]['is_muted_date'] = test.get('date_window')

                # Owner must follow the latest day in the window (tests_monitor owner can change
                # when TESTOWNERS is updated); the first row is the earliest date due to sort order.
                if to_days(test.get('date_window', 0)) > to_days(aggregated[full_name].get('owner_date', 0)):
                    aggregated[full_name]['owner'] = test.get('owner')
                    aggregated[full_name]['owner_date'] = test.get('date_window')

            # Accumulate aggregated counters.
            aggregated[full_name]['pass_count'] += test.get('pass_count', 0)
            aggregated[full_name]['fail_count'] += test.get('fail_count', 0)
            aggregated[full_name]['mute_count'] += test.get('mute_count', 0)
            aggregated[full_name]['skip_count'] += test.get('skip_count', 0)
    
    # Compute success_rate, build summary, and collapse state history for each test.
    date_window_range = f"{start_date.strftime('%Y-%m-%d')}:{today.strftime('%Y-%m-%d')}"
    for test_data in aggregated.values():
        test_data.pop('owner_date', None)
        test_data['date_window'] = date_window_range
        total_runs = test_data['pass_count'] + test_data['fail_count'] + test_data['mute_count'] + test_data['skip_count']
        if total_runs > 0:
            test_data['success_rate'] = round((test_data['pass_count'] / total_runs) * 100, 1)
        else:
            test_data['success_rate'] = 0.0
        
        # Build issue summary without duplicating state (already shown first).
        test_data['summary'] = f"p-{test_data['pass_count']}, f-{test_data['fail_count']}, m-{test_data['mute_count']}, s-{test_data['skip_count']}, total-{total_runs}"
        
        # Build state timeline with dates.
        if 'state_history' in test_data and len(test_data['state_history']) > 1:
            # Build a string with state transitions and dates.
            state_with_dates = []
            for i, (state, date) in enumerate(zip(test_data['state_history'], test_data['state_dates'])):
                if date:
                    # Convert date value into a readable format.
                    if isinstance(date, int):
                        date_obj = base_date + datetime.timedelta(days=date)
                    else:
                        date_obj = date
                    date_str = date_obj.strftime('%Y-%m-%d')
                    state_with_dates.append(f"{state}({date_str})")
                else:
                    state_with_dates.append(state)
            test_data['state'] = '->'.join(state_with_dates)
        # If there is only one state, keep it as-is.
    
    logging.info(f"Aggregation completed: {len(aggregated)} unique tests aggregated from {total_count} records")
    return list(aggregated.values())


def calculate_success_rate(test):
    """Compute success rate for a test."""
    runs = test.get('pass_count', 0) + test.get('fail_count', 0)
    return round((test.get('pass_count', 0) / runs * 100) if runs > 0 else 0, 1)

def create_test_string(test, use_wildcards=False):
    testsuite = test.get('suite_folder')
    testcase = test.get('test_name')
    test_string = f"{testsuite} {testcase}"
    if use_wildcards:
        test_string = re.sub(r'\d+/(\d+)\]', r'*/*]', test_string)
    return test_string

def sort_key_without_prefix(line):
    """Sort helper that ignores +++, ---, xxx prefixes."""
    # Strip +++, ---, xxx prefixes for sorting purposes.
    if line.startswith('+++ ') or line.startswith('--- ') or line.startswith('xxx '):
        return line[4:]  # Strip first 4 characters (prefix and space).
    return line  # Keep lines without prefixes unchanged.


def create_debug_string(test, success_rate=None, period_days=None, date_window=None):
    """Create a debug string for a test."""
    if success_rate is None:
        success_rate = calculate_success_rate(test)
    
    testsuite = test.get('suite_folder')
    testcase = test.get('test_name')
    runs = test.get('pass_count', 0) + test.get('fail_count', 0) + test.get('mute_count', 0) 
    
    debug_string = f"{testsuite} {testcase} # owner {test.get('owner', 'N/A')} success_rate {success_rate}%"
    # Show both period forms: number of days and date range when available.
    if period_days is None:
        period_days = test.get('period_days')
    if date_window is None:
        date_window = test.get('date_window')
    if period_days:
        debug_string += f" (last {period_days} days)"
    if date_window:
        debug_string += f" [{date_window}]"
    state = test.get('state', 'N/A')
    if is_chunk_test(test):
        state = f"(chunk)"
    
    is_muted = test.get('is_muted', False)
    mute_state = "muted" if is_muted else "not muted"
    debug_string += f", p-{test.get('pass_count')}, f-{test.get('fail_count')},m-{test.get('mute_count')}, s-{test.get('skip_count')}, runs-{runs}, mute state: {mute_state}, test state {state}"
    return debug_string

def is_mute_candidate(test):
    """Check whether a test is a mute candidate for the given period."""
    if test.get('is_muted', False):
        return False

    total_runs = test.get('pass_count', 0) + test.get('fail_count', 0)
    fail_count = test.get('fail_count', 0)
    result = (fail_count >= 3 and total_runs > 10) or (fail_count >= 2 and total_runs <= 10)

    logging.info(
        'MUTE_CHECK: %s - runs:%s, fails:%s, state:%s, muted:%s, result:%s',
        test.get('full_name'),
        total_runs,
        fail_count,
        test.get('state'),
        test.get('is_muted'),
        result,
    )

    return result

def is_unmute_candidate(test):
    """Check whether a test is an unmute candidate for the given period."""
    total_runs = test.get('pass_count', 0) + test.get('fail_count', 0) + test.get('mute_count', 0)
    total_fails = test.get('fail_count', 0) + test.get('mute_count', 0)

    result = total_runs >= 4 and total_fails == 0

    if test.get('is_muted', False):
        logging.info(
            'UNMUTE_CHECK: %s - runs:%s, fails:%s, mute_count:%s, state:%s, muted:%s, result:%s',
            test.get('full_name'),
            total_runs,
            total_fails,
            test.get('mute_count'),
            test.get('state'),
            test.get('is_muted'),
            result,
        )

    return result

def is_delete_candidate(test):
    """Check whether a test is a delete-from-mute candidate for the given period."""
    pass_count = test.get('pass_count', 0)
    fail_count = test.get('fail_count', 0)
    mute_count = test.get('mute_count', 0)
    skip_count = test.get('skip_count', 0)
    total_runs = pass_count + fail_count + mute_count + skip_count

    only_skipped_while_muted = (
        test.get('is_muted', False)
        and skip_count > 0
        and pass_count == 0
        and fail_count == 0
        and mute_count == 0
    )
    result = total_runs == 0 or only_skipped_while_muted

    if test.get('is_muted', False):
        logging.info(
            'DELETE_CHECK: %s - runs:%s, p:%s, f:%s, m:%s, s:%s, muted:%s, '
            'only_skipped_while_muted:%s, result:%s',
            test.get('full_name'),
            total_runs,
            pass_count,
            fail_count,
            mute_count,
            skip_count,
            test.get('is_muted'),
            only_skipped_while_muted,
            result,
        )

    return result

def create_file_set(
    aggregated_for_mute,
    filter_func,
    mute_check=None,
    use_wildcards=False,
    resolution=None,
    debug_suffix='',
):
    """Create a set of tests for output file based on a filter."""
    result_set = set()
    debug_list = []
    total = len(aggregated_for_mute)
    last_percent = -1
    for idx, test in enumerate(aggregated_for_mute, 1):
        testsuite = test.get('suite_folder')
        testcase = test.get('test_name')
        
        if not testsuite or not testcase:
            continue
        
        # Apply mute_check when provided.
        if mute_check and not mute_check(testsuite, testcase):
            continue
        # Progress bar.
        percent = int(idx / total * 100)
        if percent != last_percent and (percent % 5 == 0 or percent == 100):
            print(f"\r[create_file_set] Progress: {percent}% ({idx}/{total})", end="")
            last_percent = percent
        # Apply filter predicate.
        if filter_func(test):
            test_string = create_test_string(test, use_wildcards)
            result_set.add(test_string)
            
            if resolution:
                debug_string = create_debug_string(
                    test,
                    period_days=test.get('period_days'),
                    date_window=test.get('date_window'),
                )
                if debug_suffix:
                    debug_string += debug_suffix
                debug_list.append(debug_string)
    
    # Force 100% output if it was not printed yet.
    if last_percent != 100:
        print(f"\r[create_file_set] Progress: 100% ({total}/{total})", end="")
    print()  # Newline after progress output.
    return sorted(list(result_set)), sorted(debug_list)

def write_file_set(file_path, test_set, debug_list=None, sort_without_prefixes=False):
    # By default use regular lexicographic sorting.
    if not sort_without_prefixes:
        sorted_test_set = sorted(test_set)
    else:
        sorted_test_set = sorted(test_set, key=sort_key_without_prefix)
    
    # Sort debug list if present (same sorting mode as test_set).
    sorted_debug_list = None
    if debug_list:
        if not sort_without_prefixes:
            sorted_debug_list = sorted(debug_list)
        else:
            sorted_debug_list = sorted(debug_list, key=sort_key_without_prefix)
    
    add_lines_to_file(file_path, [line + '\n' for line in sorted_test_set])
    if sorted_debug_list:
        debug_path = file_path.replace('.txt', '_debug.txt')
        add_lines_to_file(debug_path, [line + '\n' for line in sorted_debug_list])
    logging.info(f"Created {os.path.basename(file_path)} with {len(sorted_test_set)} tests")

def apply_and_add_mutes(
    all_data,
    output_path,
    mute_check,
    aggregated_for_mute,
    aggregated_for_unmute,
    aggregated_for_delete,
    manual_unmute_full_names=None,
    aggregated_for_manual_unmute=None,
    manual_unmute_min_runs=None,
    manual_unmute_window_days=None,
    ydb_wrapper=None,
    branch=None,
    build_type=None,
):
    output_path = os.path.join(output_path, 'mute_update')
    logging.info(f"Creating mute files in directory: {output_path}")
    
    # Process all tests loaded from the monitor query.

    # Collect stats for currently muted tests.
    muted_tests = [test for test in aggregated_for_mute if test.get('is_muted', False)]
    logging.info(f"Total muted tests found: {len(muted_tests)}")


    try:
        # 1. Mute candidates.
        
        to_mute, to_mute_debug = create_file_set(
            aggregated_for_mute, is_mute_candidate, use_wildcards=True, resolution='to_mute'
        )

        # 2. Unmute candidates.
        def is_unmute_non_chunk(test):
            if is_chunk_test(test):
                return False
            return is_unmute_candidate(test)

        to_unmute, to_unmute_debug = create_file_set(
            aggregated_for_unmute, is_unmute_non_chunk, mute_check, resolution='to_unmute'
        )
        
        # Wildcard unmute patterns:
        # If all pattern chunks pass the filter, unmute the whole pattern.
        # If at least one chunk fails, the pattern is not unmuted.
        wildcard_unmute = get_wildcard_unmute_candidates(aggregated_for_unmute, mute_check, is_unmute_candidate)
        wildcard_unmute_patterns = [p for p, d in wildcard_unmute]
        wildcard_unmute_debugs = [d for p, d in wildcard_unmute]
        
        # Merge per-test and wildcard results.
        to_unmute = sorted(list(set(to_unmute) | set(wildcard_unmute_patterns)))
        to_unmute_debug = sorted(list(set(to_unmute_debug) | set(wildcard_unmute_debugs)))

        # 2a. Manual fast-unmute candidates.
        # A test is considered under manual fast-unmute when its full_name is
        # registered in `fast_unmute_active` (populated when a
        # user manually closes the mute issue). Such tests are evaluated on a
        # shorter window and smaller min_runs threshold, so they get unmuted
        # sooner when stable.
        manual_unmute_full_names = set(manual_unmute_full_names or [])
        if manual_unmute_full_names and aggregated_for_manual_unmute and manual_unmute_min_runs:
            def _manual_rows_to_unmute_output(rows, debug_suffix):
                """Build sorted mute lines + debug lines (same shape as ``create_file_set`` output)."""
                result_set = set()
                debug_list = []
                for test in rows:
                    test_string = create_test_string(test, use_wildcards=False)
                    result_set.add(test_string)
                    debug_string = create_debug_string(
                        test,
                        period_days=test.get('period_days'),
                        date_window=test.get('date_window'),
                    )
                    debug_list.append(debug_string + debug_suffix)
                return sorted(list(result_set)), sorted(debug_list)

            # Single pass: partition manual-unmute rows and log once per tracked test.
            manual_stable_rows = []
            manual_zero_rows = []
            _mu_total = len(aggregated_for_manual_unmute)
            _mu_last_pct = -1
            for _mu_idx, test in enumerate(aggregated_for_manual_unmute, 1):
                testsuite = test.get('suite_folder')
                testcase = test.get('test_name')
                if not testsuite or not testcase:
                    continue
                if mute_check and not mute_check(testsuite, testcase):
                    continue
                _mu_pct = int(_mu_idx / _mu_total * 100)
                if _mu_pct != _mu_last_pct and (_mu_pct % 5 == 0 or _mu_pct == 100):
                    print(
                        f"\r[manual fast-unmute] Progress: {_mu_pct}% ({_mu_idx}/{_mu_total})",
                        end="",
                    )
                    _mu_last_pct = _mu_pct

                fn = test.get('full_name')
                if is_chunk_test(test) or fn not in manual_unmute_full_names:
                    continue
                p = test.get('pass_count', 0)
                f = test.get('fail_count', 0)
                m = test.get('mute_count', 0)
                s = test.get('skip_count', 0)
                total_runs_pf_m = p + f + m
                total_activity = p + f + m + s
                total_fails = f + m
                win = manual_unmute_window_days if manual_unmute_window_days is not None else '?'
                if total_activity == 0:
                    logging.info(
                        'FAST_UNMUTE_CHECK: %s - runs(p+f+m):%s, activity(p+f+m+s):%s, fails:%s, '
                        'min_runs:%s, window_days=%s, path:fast-delete, result:True',
                        fn,
                        total_runs_pf_m,
                        total_activity,
                        total_fails,
                        manual_unmute_min_runs,
                        win,
                    )
                    manual_zero_rows.append(test)
                    continue
                stable_ok = total_runs_pf_m >= manual_unmute_min_runs and total_fails == 0
                logging.info(
                    'FAST_UNMUTE_CHECK: %s - runs(p+f+m):%s, fails:%s, min_runs:%s, window_days=%s, '
                    'path:fast-unmute, result:%s',
                    fn,
                    total_runs_pf_m,
                    total_fails,
                    manual_unmute_min_runs,
                    win,
                    stable_ok,
                )
                if stable_ok:
                    manual_stable_rows.append(test)

            if _mu_last_pct != 100:
                print(
                    f"\r[manual fast-unmute] Progress: 100% ({_mu_total}/{_mu_total})",
                    end="",
                )
            print()

            to_unmute_manual_stable, to_unmute_manual_stable_debug = _manual_rows_to_unmute_output(
                manual_stable_rows,
                ' [fast-unmute]',
            )
            to_unmute_manual_zero_activity, to_unmute_manual_zero_activity_debug = _manual_rows_to_unmute_output(
                manual_zero_rows,
                ' [fast-delete]',
            )
            to_unmute_manual = sorted(
                set(to_unmute_manual_stable) | set(to_unmute_manual_zero_activity)
            )
            to_unmute_manual_debug = sorted(
                set(to_unmute_manual_stable_debug) | set(to_unmute_manual_zero_activity_debug)
            )
            if to_unmute_manual:
                logging.info(
                    'Manual fast-unmute added %d test(s) to to_unmute '
                    '(%d stable [fast-unmute], %d zero-runs [fast-delete])',
                    len(to_unmute_manual),
                    len(to_unmute_manual_stable),
                    len(to_unmute_manual_zero_activity),
                )
            to_unmute = sorted(list(set(to_unmute) | set(to_unmute_manual)))
            to_unmute_debug = sorted(list(set(to_unmute_debug) | set(to_unmute_manual_debug)))

        write_file_set(os.path.join(output_path, 'to_mute.txt'), to_mute, to_mute_debug)
        write_file_set(os.path.join(output_path, 'to_unmute.txt'), to_unmute, to_unmute_debug)

        # 3. Delete-from-mute candidates (to_delete).
        def is_delete_non_chunk(test):
            if is_chunk_test(test):
                return False
            return is_delete_candidate(test)

        to_delete, to_delete_debug = create_file_set(
            aggregated_for_delete, is_delete_non_chunk, mute_check, resolution='to_delete'
        )
        
        # Wildcard delete patterns:
        # If all pattern chunks pass the filter, delete mute for the whole pattern.
        # If at least one chunk fails, do not delete the pattern.
        wildcard_delete = get_wildcard_delete_candidates(aggregated_for_delete, mute_check, is_delete_candidate)
        wildcard_delete_patterns = [p for p, d in wildcard_delete]
        wildcard_delete_debugs = [d for p, d in wildcard_delete]
        
        # Merge per-test and wildcard results.
        to_delete = sorted(list(set(to_delete) | set(wildcard_delete_patterns)))
        to_delete_debug = sorted(list(set(to_delete_debug) | set(wildcard_delete_debugs)))
        
        write_file_set(os.path.join(output_path, 'to_delete.txt'), to_delete, to_delete_debug)
        
        # 4. muted_ya (all currently muted tests).
        all_muted_ya, all_muted_ya_debug = create_file_set(
            all_data, lambda test: mute_check(test.get('suite_folder'), test.get('test_name')) if mute_check else True, use_wildcards=True, resolution='muted_ya'
        )
        write_file_set(os.path.join(output_path, 'muted_ya.txt'), all_muted_ya, all_muted_ya_debug)
        to_mute_set = set(to_mute)
        to_unmute_set = set(to_unmute)
        to_delete_set = set(to_delete)
        all_muted_ya_set = set(all_muted_ya)
        
         # Build maps for fast debug string lookup.

        # Universal map: key is test string (with or without wildcard), value is debug string.
        test_debug_dict = {}
        wildcard_to_chunks = defaultdict(list)
        for test in aggregated_for_mute + aggregated_for_unmute + aggregated_for_delete:
            test_str = create_test_string(test, use_wildcards=False)  # Keep raw string, no .strip().
            debug_str = create_debug_string(test)
            test_debug_dict[test_str] = debug_str
            if is_chunk_test(test):
                wildcard_key = create_test_string(test, use_wildcards=True)
                wildcard_to_chunks[wildcard_key].append(test)
        # Build wildcard-level debug strings.
        for wildcard, chunks in wildcard_to_chunks.items():
            N = len(chunks)
            m = sum(1 for t in chunks if t.get('is_muted'))
            
            # Aggregate chunk stats to explain the decision.
            total_pass = sum(t.get('pass_count', 0) for t in chunks)
            total_fail = sum(t.get('fail_count', 0) for t in chunks)
            total_mute = sum(t.get('mute_count', 0) for t in chunks)
            total_skip = sum(t.get('skip_count', 0) for t in chunks)
            total_runs = total_pass + total_fail + total_mute + total_skip
            
            # Determine why this wildcard appeared in the output.
            reason = ""
            if wildcard in to_mute_set:
                reason = f"TO_MUTE: {total_fail} fails in {total_runs} runs"
            elif wildcard in to_unmute_set:
                reason = f"TO_UNMUTE: {total_fail + total_mute} fails in {total_runs} runs"
            elif wildcard in to_delete_set:
                reason = f"TO_DELETE: {total_runs} total runs"
            
            debug_str = f"{wildcard}: {N} chunks, {m} muted ({reason})"
            test_debug_dict[wildcard] = debug_str

        # 5. muted_ya+to_mute
        muted_ya_plus_to_mute = list(all_muted_ya) + [t for t in to_mute if t not in all_muted_ya]
        muted_ya_plus_to_mute_debug = []
        for test in muted_ya_plus_to_mute:
            debug_val = test_debug_dict.get(test, "NO DEBUG INFO")
            muted_ya_plus_to_mute_debug.append(debug_val)
        write_file_set(os.path.join(output_path, 'muted_ya+to_mute.txt'), muted_ya_plus_to_mute, muted_ya_plus_to_mute_debug)

        # 6. muted_ya-to_unmute
        muted_ya_minus_to_unmute = [t for t in all_muted_ya if t not in to_unmute]
        muted_ya_minus_to_unmute_debug = []
        for test in muted_ya_minus_to_unmute:
            debug_val = test_debug_dict.get(test, "NO DEBUG INFO")
            muted_ya_minus_to_unmute_debug.append(debug_val)
        write_file_set(os.path.join(output_path, 'muted_ya-to_unmute.txt'), muted_ya_minus_to_unmute, muted_ya_minus_to_unmute_debug)

        # 7. muted_ya-to_delete
        muted_ya_minus_to_delete = [t for t in all_muted_ya if t not in to_delete]
        muted_ya_minus_to_delete_debug = []
        for test in muted_ya_minus_to_delete:
            debug_val = test_debug_dict.get(test, "NO DEBUG INFO")
            muted_ya_minus_to_delete_debug.append(debug_val)
        write_file_set(os.path.join(output_path, 'muted_ya-to_delete.txt'), muted_ya_minus_to_delete, muted_ya_minus_to_delete_debug)

        # 8. muted_ya-to-delete-to-unmute
        muted_ya_minus_to_delete_to_unmute = [t for t in all_muted_ya if t not in to_delete and t not in to_unmute]
        muted_ya_minus_to_delete_to_unmute_debug = []
        for test in muted_ya_minus_to_delete_to_unmute:
            debug_val = test_debug_dict.get(test, "NO DEBUG INFO")
            muted_ya_minus_to_delete_to_unmute_debug.append(debug_val)
        write_file_set(os.path.join(output_path, 'muted_ya-to-delete-to-unmute.txt'), muted_ya_minus_to_delete_to_unmute, muted_ya_minus_to_delete_to_unmute_debug)

        # 9. muted_ya-to-delete-to-unmute+to_mute
        # Use list instead of set to preserve full ordering/compatibility.
        muted_ya_minus_to_delete_to_unmute_plus_to_mute = list(muted_ya_minus_to_delete_to_unmute) + [t for t in to_mute if t not in muted_ya_minus_to_delete_to_unmute]
        muted_ya_minus_to_delete_to_unmute_plus_to_mute_debug = []
        for test in muted_ya_minus_to_delete_to_unmute_plus_to_mute:
            debug_val = test_debug_dict.get(test, "NO DEBUG INFO")
            muted_ya_minus_to_delete_to_unmute_plus_to_mute_debug.append(debug_val)
        write_file_set(os.path.join(output_path, 'muted_ya-to-delete-to-unmute+to_mute.txt'), muted_ya_minus_to_delete_to_unmute_plus_to_mute, muted_ya_minus_to_delete_to_unmute_plus_to_mute_debug)
        # Save the same content as new_muted_ya.txt for workflow compatibility.
        write_file_set(os.path.join(output_path, 'new_muted_ya.txt'), muted_ya_minus_to_delete_to_unmute_plus_to_mute, muted_ya_minus_to_delete_to_unmute_plus_to_mute_debug)
        
        # 10. muted_ya_changes - changes file (new logic).
        all_test_strings = sorted(all_muted_ya_set | to_mute_set | to_unmute_set | to_delete_set, key=sort_key_without_prefix)
        # Ensure 1:1 correspondence between .txt and debug.txt.
        muted_ya_changes = []
        muted_ya_changes_debug = []
        for test_str in all_test_strings:  # all_test_strings must be a list, not a set.
            if test_str in to_mute_set and test_str not in all_muted_ya_set:
                prefix = "+++"
            elif test_str in to_unmute_set:
                prefix = "---"
            elif test_str in to_delete_set:
                prefix = "xxx"
            else:
                prefix = ""
            line = f"{prefix} {test_str}" if prefix else f"{test_str}"
            muted_ya_changes.append(line)
            debug_val = test_debug_dict.get(test_str, "NO DEBUG INFO")
            muted_ya_changes_debug.append(f"{prefix} {debug_val}" if prefix else debug_val)
        write_file_set(os.path.join(output_path, 'muted_ya_changes.txt'), muted_ya_changes, muted_ya_changes_debug, sort_without_prefixes=True)
        
        # Log final counters.
        logging.info(f"To mute: {len(to_mute)}")
        logging.info(f"To unmute: {len(to_unmute)}")
        logging.info(f"To delete: {len(to_delete)}")
        logging.info(f"Muted_ya: {len(all_muted_ya)}")
        logging.info(f"muted_ya+to_mute: {len(muted_ya_plus_to_mute)}")
        logging.info(f"muted_ya-to_unmute: {len(muted_ya_minus_to_unmute)}")
        logging.info(f"muted_ya-to_delete: {len(muted_ya_minus_to_delete)}")
        logging.info(f"muted_ya-to_delete-to_unmute: {len(muted_ya_minus_to_delete_to_unmute)}")
        logging.info(f"muted_ya-to_delete-to_unmute+to_mute: {len(muted_ya_minus_to_delete_to_unmute_plus_to_mute)}")
        
    except (KeyError, TypeError) as e:
        logging.error(f"Error processing test data: {e}. Check your query results for valid keys.")
        return []

    return len(to_mute)



def read_tests_from_file(file_path):
    result = []
    with open(file_path, "r") as fp:
        for line in fp:
            line = line.strip()
            try:
                testsuite, testcase = line.split(" ", maxsplit=1)
                result.append({'testsuite': testsuite, 'testcase': testcase, 'full_name': f"{testsuite}/{testcase}"})
            except ValueError:
                logging.warning(f"cant parse line: {line!r}")
                continue
    return result


def _format_issue_date_window(value):
    if value is None:
        return 'N/A'
    if isinstance(value, datetime.datetime):
        return value.date().strftime('%Y-%m-%d')
    if isinstance(value, datetime.date):
        return value.strftime('%Y-%m-%d')
    if isinstance(value, int):
        base_date = datetime.date(1970, 1, 1)
        return (base_date + datetime.timedelta(days=value)).strftime('%Y-%m-%d')
    return str(value)


def _compute_summary_from_counts(row):
    pass_count = int(row.get('pass_count') or 0)
    fail_count = int(row.get('fail_count') or 0)
    mute_count = int(row.get('mute_count') or 0)
    skip_count = int(row.get('skip_count') or 0)
    total_runs = pass_count + fail_count + mute_count + skip_count
    return f"p-{pass_count}, f-{fail_count}, m-{mute_count}, s-{skip_count}, total-{total_runs}"


def create_mute_issues(
    all_tests,
    file_path,
    aggregated_tests,
    close_issues=True,
    branch='main',
    build_type=DEFAULT_BUILD_TYPE,
):
    tests_from_file = read_tests_from_file(file_path)
    issues_index = get_issues_and_tests_from_project(ORG_NAME, PROJECT_ID)
    muted_tests_in_issues = get_muted_tests_from_issues(issues_index)
    manual_fast_unmute_issue_by_test = map_tests_to_manual_fast_unmute_issue_url(issues_index)
    prepared_tests_by_suite = {}
    temp_tests_by_suite = {}
    
    # Create set of muted tests for faster lookup
    muted_tests_set = {test['full_name'] for test in tests_from_file}
    
    # Check and close issues if needed
    closed_issues = []
    partially_unmuted_issues = []
    if close_issues:
        closed_issues, partially_unmuted_issues = close_unmuted_issues(muted_tests_set)
    
    monitor_by_name = {}
    for t in sorted(all_tests, key=lambda x: x.get('date_window') or 0):
        if t.get('full_name'):
            bt = t.get('build_type') or DEFAULT_BUILD_TYPE
            monitor_by_name[(t['full_name'], bt)] = t
    aggregated_by_name = {}
    for t in aggregated_tests:
        if t.get('full_name'):
            bt = t.get('build_type') or DEFAULT_BUILD_TYPE
            aggregated_by_name[(t['full_name'], bt)] = t

    for test_from_file in tests_from_file:
        full_name = test_from_file['full_name']

        # Do not create issues for chunks and wildcard rows in muted_ya.
        if '*/*' in test_from_file.get('testcase', ''):
            logging.info(f"Skipping chunk wildcard pattern: {full_name}")
            continue

        issue_key = (full_name, build_type)
        if issue_key in muted_tests_in_issues:
            logging.info(
                f"test {full_name} ({build_type}) already have issue, {muted_tests_in_issues[issue_key][0]['url']}"
            )
            continue
        if issue_key in manual_fast_unmute_issue_by_test:
            logging.info(
                'test %s (%s) skipped: existing issue with manual-fast-unmute label: %s',
                full_name,
                build_type,
                manual_fast_unmute_issue_by_test[issue_key],
            )
            continue

        monitor = monitor_by_name.get((full_name, build_type))
        aggregated = aggregated_by_name.get((full_name, build_type))
        if monitor and is_chunk_test(monitor):
            logging.info(f"Skipping chunk test: {full_name}")
            continue
        if monitor:
            if not aggregated:
                logging.warning(
                    "No aggregated row for %s (%s): using raw monitor fields",
                    full_name,
                    build_type,
                )
            days_in_state = monitor.get('days_in_state')
            if days_in_state is None:
                logging.warning(
                    "Raw monitor row for %s (%s) has no days_in_state: using 0",
                    full_name,
                    build_type,
                )
                days_in_state = 0
            source = aggregated or monitor
            success_rate = source.get('success_rate')
            if success_rate is None:
                pass_count = int(source.get('pass_count') or 0)
                fail_count = int(source.get('fail_count') or 0)
                mute_count = int(source.get('mute_count') or 0)
                skip_count = int(source.get('skip_count') or 0)
                total_runs = pass_count + fail_count + mute_count + skip_count
                success_rate = round((pass_count / total_runs) * 100, 1) if total_runs > 0 else 0.0
            summary = source.get('summary') or _compute_summary_from_counts(source)
            state = source.get('state') or monitor.get('state') or 'Muted'
            date_window = source.get('date_window') or monitor.get('date_window')

            entry = {
                'mute_string': f"{monitor.get('suite_folder')} {monitor.get('test_name')}",
                'test_name': monitor.get('test_name'),
                'suite_folder': monitor.get('suite_folder'),
                'full_name': full_name,
                'success_rate': success_rate,
                'days_in_state': days_in_state,
                'date_window': _format_issue_date_window(date_window),
                'owner': monitor.get('owner'),
                'state': state,
                'summary': summary,
                'fail_count': source.get('fail_count'),
                'pass_count': source.get('pass_count'),
                'branch': monitor.get('branch'),
                'build_type': monitor.get('build_type', DEFAULT_BUILD_TYPE),
            }
        else:
            entry = {
                'mute_string': f"{test_from_file['testsuite']} {test_from_file['testcase']}",
                'test_name': test_from_file['testcase'],
                'suite_folder': test_from_file['testsuite'],
                'full_name': full_name,
                'success_rate': 0,
                'days_in_state': 0,
                'date_window': 'N/A',
                'owner': 'unknown',
                'state': 'Muted',
                'summary': 'added manually, no monitor data',
                'fail_count': 0,
                'pass_count': 0,
                'branch': branch,
                'build_type': build_type,
            }
            logging.info(f"test {full_name} not in monitor, using fallback data")

        key = f"{test_from_file['testsuite']}:{canonical_team_slug(entry['owner'])}"
        if not temp_tests_by_suite.get(key):
            temp_tests_by_suite[key] = []
        temp_tests_by_suite[key].append(entry)
    
    # Split groups larger than 40 tests
    for key, tests in temp_tests_by_suite.items():
        if len(tests) <= 40:
            prepared_tests_by_suite[key] = tests
        else:
            # Split into groups of 40
            for i in range(0, len(tests), 40):
                chunk = tests[i:i+40]
                chunk_key = f"{key}_{i//40 + 1}"  # Add iterator to key starting from 1
                prepared_tests_by_suite[chunk_key] = chunk

    results = []
    queue_items = []
    for item in prepared_tests_by_suite:
        title, body = generate_github_issue_title_and_body(prepared_tests_by_suite[item])
        raw_owner = prepared_tests_by_suite[item][0]['owner']
        owner_value = canonical_team_slug(raw_owner)
        result = create_and_add_issue_to_project(title, body, state='Muted', owner=owner_value)
        if not result:
            break
        else:
            issue_url = result['issue_url']
            results.append(
                {
                    'message': f"Created issue '{title}' for TEAM:@ydb-platform/{owner_value}, url {issue_url}",
                    'owner': owner_value
                }
            )
            try:
                issue_number = int(issue_url.rstrip('/').split('/')[-1])
            except (ValueError, IndexError):
                issue_number = None
            if issue_number:
                first_test = prepared_tests_by_suite[item][0]
                queue_items.append({
                    'github_issue_number': issue_number,
                    'github_issue_url': issue_url,
                    'github_issue_title': title,
                    'owner_team': owner_value,
                    'branch': first_test.get('branch', 'main'),
                    'build_type': first_test.get('build_type', DEFAULT_BUILD_TYPE),
                })

    # Sort results by owner
    results.sort(key=lambda x: x['owner'])
    
    # Group results by owner and add spacing and headers
    formatted_results = []
    
    # Add closed issues section if any
    if closed_issues:
        formatted_results.append("🔒 **CLOSED ISSUES**")
        formatted_results.append("─────────────────────────────")
        formatted_results.append("")
        for issue in closed_issues:
            formatted_results.append(f"✅ **Closed** {issue['url']}")
            formatted_results.append("   📝 **Unmuted tests:**")
            for test in issue['tests']:
                formatted_results.append(f"   • `{test}`")
            formatted_results.append("")
    
    # Add partially unmuted issues section if any
    if partially_unmuted_issues:
        if closed_issues:
            formatted_results.append("─────────────────────────────")
            formatted_results.append("")
        formatted_results.append("🔓 **PARTIALLY UNMUTED ISSUES**")
        formatted_results.append("─────────────────────────────")
        formatted_results.append("")
        for issue in partially_unmuted_issues:
            formatted_results.append(f"⚠️ **Partially unmuted** {issue['url']}")
            formatted_results.append("   📝 **Unmuted tests:**")
            for test in issue['unmuted_tests']:
                formatted_results.append(f"   • `{test}`")
            formatted_results.append("")
    
    # Add created issues section if any
    if results:
        if closed_issues or partially_unmuted_issues:
            formatted_results.append("─────────────────────────────")
            formatted_results.append("")
        formatted_results.append("🆕 **CREATED ISSUES**")
        formatted_results.append("─────────────────────────────")
        formatted_results.append("")
    
        # Add created issues
        current_owner = None
        for result in results:
            if current_owner != result['owner']:
                if formatted_results and formatted_results[-1] != "":
                    formatted_results.append('')
                    formatted_results.append('')
                current_owner = result['owner']
                # Add owner header with team URL
                formatted_results.append(f"👥 **TEAM** @ydb-platform/{current_owner}")
                formatted_results.append(f"   https://github.com/orgs/ydb-platform/teams/{current_owner}")
                formatted_results.append("   ┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄")
            
            # Extract issue URL and title
            issue_url = result['message'].split('url ')[-1]
            title = result['message'].split("'")[1]
            formatted_results.append(f"   🎯 {issue_url} - `{title}`")

    print("\n\n")
    print("\n".join(formatted_results))
    if 'GITHUB_OUTPUT' in os.environ:
        if 'GITHUB_WORKSPACE' not in os.environ:
            raise EnvironmentError("GITHUB_WORKSPACE environment variable is not set.")
        
        file_path = os.path.join(os.environ['GITHUB_WORKSPACE'], "created_issues.txt")
        print(f"Writing results to {file_path}")
        
        with open(file_path, 'w') as f:
            f.write("\n")
            f.write("\n".join(formatted_results))
            f.write("\n")
            
        with open(os.environ['GITHUB_OUTPUT'], 'a') as gh_out:
            gh_out.write(f"created_issues_file={file_path}\n")
            
        print(f"Result saved to env variable GITHUB_OUTPUT by key created_issues_file")

    return queue_items


def _load_issue_digest_profiles():
    try:
        with open(_DIGEST_NOTIFICATION_CONFIG, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except FileNotFoundError:
        logging.warning(
            'Digest config not found at %s — not enqueueing to digest_queue',
            _DIGEST_NOTIFICATION_CONFIG,
        )
        return set()
    except json.JSONDecodeError as exc:
        logging.warning(
            'Invalid digest config %s: %s — not enqueueing to digest_queue',
            _DIGEST_NOTIFICATION_CONFIG,
            exc,
        )
        return []
    return data.get('profiles') or []


def load_configured_issue_profile_ids():
    """Profiles allowed for issue sync (branch:build_type)."""
    profiles = _load_issue_digest_profiles()
    ids = {
        make_profile_id(p['branch'], p['build_type'])
        for p in profiles
        if p.get('branch') and p.get('build_type')
    }
    if not ids:
        logging.warning(
            'No profiles in %s — skipping issue creation',
            _DIGEST_NOTIFICATION_CONFIG,
        )
    return ids


def load_configured_digest_profile_ids():
    """Profiles allowed for digest queue enqueue (branch:build_type).

    Enqueue is enabled only for profiles that define digest schedule
    via non-empty "schedule_utc_hours".
    """
    profiles = _load_issue_digest_profiles()
    ids = {
        make_profile_id(p['branch'], p['build_type'])
        for p in profiles
        if p.get('branch')
        and p.get('build_type')
        and isinstance(p.get('schedule_utc_hours'), list)
        and len(p.get('schedule_utc_hours')) > 0
    }
    if not ids:
        logging.warning(
            'No profiles with non-empty schedule_utc_hours in %s — not enqueueing to digest_queue',
            _DIGEST_NOTIFICATION_CONFIG,
        )
    return ids


def enqueue_to_digest_queue(ydb_wrapper, queue_items):
    """Write newly created issues into digest_queue so send_digest.py can find them."""
    if not queue_items:
        return

    allowed_profiles = load_configured_digest_profile_ids()
    if not allowed_profiles:
        logging.info('No digest profiles configured — skip digest_queue enqueue')
        return

    filtered = []
    for item in queue_items:
        pid = make_profile_id(item['branch'], item['build_type'])
        if pid in allowed_profiles:
            filtered.append(item)
        else:
            logging.info(
                'Skip digest enqueue for issue #%s (profile %r not in mute_issue_and_digest_config.json)',
                item.get('github_issue_number'),
                pid,
            )
    if not filtered:
        logging.info('No queue items match configured digest profiles — nothing enqueued')
        return

    try:
        table_path = ydb_wrapper.get_table_path("digest_queue")
    except KeyError:
        logging.warning("digest_queue not found in YDB config — skipping enqueue")
        return

    create_table_sql = """\
CREATE TABLE IF NOT EXISTS `{table_path}` (
    `profile_id`          Utf8      NOT NULL,
    `github_issue_number` Uint64    NOT NULL,
    `github_issue_url`    Utf8,
    `github_issue_title`  Utf8,
    `owner_team`          Utf8,
    `branch`              Utf8,
    `build_type`          Utf8,
    `enqueued_at`         Timestamp NOT NULL,
    `sent_at`             Timestamp,
    PRIMARY KEY (profile_id, github_issue_number)
)
WITH (
    STORE = COLUMN
)
"""
    ydb_wrapper.create_table(
        table_path,
        create_table_sql.format(table_path=table_path),
    )

    now_dt = datetime.datetime.now(tz=datetime.timezone.utc)

    rows = []
    for item in filtered:
        branch = item['branch']
        build_type = item['build_type']
        profile_id = make_profile_id(branch, build_type)
        rows.append({
            'profile_id':          profile_id,
            'github_issue_number': item['github_issue_number'],
            'github_issue_url':    item['github_issue_url'],
            'github_issue_title':  item['github_issue_title'],
            'owner_team':          item['owner_team'],
            'branch':              branch,
            'build_type':          build_type,
            'enqueued_at':         now_dt,
            'sent_at':             None,
        })

    column_types = (
        ydb.BulkUpsertColumns()
        .add_column('profile_id',          ydb.PrimitiveType.Utf8)
        .add_column('github_issue_number', ydb.PrimitiveType.Uint64)
        .add_column('github_issue_url',    ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column('github_issue_title',  ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column('owner_team',          ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column('branch',              ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column('build_type',          ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column('enqueued_at',         ydb.PrimitiveType.Timestamp)
        .add_column('sent_at',             ydb.OptionalType(ydb.PrimitiveType.Timestamp))
    )

    ydb_wrapper.bulk_upsert_batches(
        table_path, rows, column_types, query_name="enqueue_digest_items"
    )
    logging.info(f"Enqueued {len(rows)} issue(s) into digest_queue")


def mute_worker(args):
    with YDBWrapper() as ydb_wrapper:
        # Check credentials
        if not ydb_wrapper.check_credentials():
            return 1

        build_type = getattr(args, 'build_type', DEFAULT_BUILD_TYPE)
        logging.info(f"Starting mute worker with mode: {args.mode}")
        logging.info(f"Branch: {args.branch}")
        logging.info(f"build_type: {build_type}")

        mute_window_days = get_mute_window_days()
        unmute_window_days = get_unmute_window_days()
        delete_window_days = get_delete_window_days()

        all_data = execute_query(args.branch, build_type=build_type, ydb_wrapper=ydb_wrapper)
        logging.info(f"Query returned {len(all_data)} test records")
        
        # Use unified aggregation for different periods.
        aggregated_for_mute = aggregate_test_data(all_data, mute_window_days)
        aggregated_for_unmute = aggregate_test_data(all_data, unmute_window_days)
        aggregated_for_delete = aggregate_test_data(all_data, delete_window_days)

        manual_unmute_window_days, manual_unmute_min_runs = load_manual_unmute_config()
        manual_unmute_full_names = set()
        aggregated_for_manual_unmute = None
        if manual_unmute_window_days and manual_unmute_min_runs:
            manual_unmute_full_names = load_manual_unmute_full_names(ydb_wrapper, args.branch, build_type)
            if manual_unmute_full_names:
                aggregated_for_manual_unmute = aggregate_test_data(all_data, manual_unmute_window_days)
                logging.info(
                    f"Manual fast-unmute: window={manual_unmute_window_days}d, min_runs={manual_unmute_min_runs}, "
                    f"tests={len(manual_unmute_full_names)}"
                )

        logging.info(f"Aggregated data: mute={len(aggregated_for_mute)}, unmute={len(aggregated_for_unmute)}, delete={len(aggregated_for_delete)}")

        grace_map = load_fast_unmute_grace_map(ydb_wrapper, args.branch, build_type)
        if grace_map:
            ladder_base = manual_unmute_window_days or 2
            aggregated_for_mute = merge_mute_aggregate_with_fast_unmute_grace(
                all_data,
                aggregated_for_mute,
                grace_map,
                ladder_base,
                mute_window_days,
            )
            logging.info(
                'Fast-unmute grace ladder: %d test(s), effective mute window min(%d, %d + days_since_grace)',
                len(grace_map),
                mute_window_days,
                ladder_base,
            )

        if args.mode == 'update_muted_ya':
            # update_muted_ya uses mute rules to build output files,
            # so only this mode requires loading muted_ya into YaMuteCheck.
            input_muted_ya_path = resolve_muted_ya_path(getattr(args, 'muted_ya_file', ''), build_type)
            logging.info(f"Using muted_ya file: {input_muted_ya_path}")
            mute_check = YaMuteCheck()
            mute_check.load(input_muted_ya_path)
            logging.info(f"Loaded muted_ya.txt with {len(mute_check.regexps)} test patterns")

            output_path = args.output_folder
            os.makedirs(output_path, exist_ok=True)
            logging.info(f"Creating mute files in: {output_path}")
            apply_and_add_mutes(
                all_data,
                output_path,
                mute_check,
                aggregated_for_mute,
                aggregated_for_unmute,
                aggregated_for_delete,
                manual_unmute_full_names=manual_unmute_full_names,
                aggregated_for_manual_unmute=aggregated_for_manual_unmute,
                manual_unmute_min_runs=manual_unmute_min_runs,
                manual_unmute_window_days=manual_unmute_window_days,
                ydb_wrapper=ydb_wrapper,
                branch=args.branch,
                build_type=build_type,
            )

        elif args.mode == 'sync_fast_unmute_grace':
            to_mute, _ = create_file_set(
                aggregated_for_mute, is_mute_candidate, use_wildcards=True, resolution='to_mute'
            )
            delete_fast_unmute_grace_rows(ydb_wrapper, args.branch, build_type, to_mute)
            logging.info(
                "fast_unmute_grace cleanup completed for branch=%s build_type=%s; candidates=%d",
                args.branch,
                build_type,
                len(to_mute),
            )

        elif args.mode == 'create_issues':
            file_path = resolve_muted_ya_path(getattr(args, 'file_path', ''), build_type)
            logging.info(f"Creating issues from file: {file_path}")

            profile_id = make_profile_id(args.branch, build_type)
            allowed_profiles = load_configured_issue_profile_ids()
            if profile_id not in allowed_profiles:
                logging.info(
                    f"Profile {profile_id!r} not in mute_issue_and_digest_config.json — skipping issue creation"
                )
                return 0

            queue_items = create_mute_issues(
                all_data,
                file_path,
                aggregated_tests=aggregated_for_mute,
                close_issues=args.close_issues,
                branch=args.branch,
                build_type=build_type,
            )

            try:
                enqueue_to_digest_queue(ydb_wrapper, queue_items or [])
            except Exception as exc:
                logging.warning(f"Failed to enqueue issues for digest: {exc}")

        logging.info("Mute worker completed successfully")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Add tests to mutes files based on flaky_today condition")

    subparsers = parser.add_subparsers(dest='mode', help="Mode to perform")

    update_muted_ya_parser = subparsers.add_parser('update_muted_ya', help='create new muted_ya')
    update_muted_ya_parser.add_argument('--output_folder', default=repo_path, required=False, help='Output folder.')
    update_muted_ya_parser.add_argument('--branch', default='main', help='Branch to get history')
    update_muted_ya_parser.add_argument(
        '--muted_ya_file',
        default='',
        help='Path to input muted_ya.txt file (default: resolved from build-type policy)',
    )
    update_muted_ya_parser.add_argument(
        '--build-type',
        default=DEFAULT_BUILD_TYPE,
        dest='build_type',
        help='tests_monitor build_type slice (default: relwithdebinfo)',
    )

    sync_fast_unmute_grace_parser = subparsers.add_parser(
        'sync_fast_unmute_grace',
        help='remove fast_unmute_grace rows for tests that are mute candidates again',
    )
    sync_fast_unmute_grace_parser.add_argument('--branch', default='main', help='Branch to get history')
    sync_fast_unmute_grace_parser.add_argument(
        '--build-type',
        default=DEFAULT_BUILD_TYPE,
        dest='build_type',
        help='tests_monitor build_type slice (default: relwithdebinfo)',
    )

    create_issues_parser = subparsers.add_parser(
        'create_issues',
        help='sync issues with muted_ya file: create missing, close orphaned, enqueue to digest',
    )
    create_issues_parser.add_argument(
        '--file_path',
        default='',
        required=False,
        help='Path to muted_ya.txt (default: resolved from build-type policy)',
    )
    create_issues_parser.add_argument('--branch', default='main', help='Branch to get history')
    create_issues_parser.add_argument('--close_issues', action=argparse.BooleanOptionalAction, default=True, help='Close issues when all tests are unmuted (default: True)')
    create_issues_parser.add_argument(
        '--build-type',
        default=DEFAULT_BUILD_TYPE,
        dest='build_type',
        help='tests_monitor build_type when loading monitor rows (default: relwithdebinfo)',
    )

    args = parser.parse_args()

    mute_worker(args)
