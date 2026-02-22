#!/usr/bin/env python3
"""
Legacy mute pipeline: reads from tests_monitor, hardcoded rules.
No quarantine, no graduation, no pattern_rules, no mute_decisions.
"""

import argparse
import datetime
import logging
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'analytics'))

from mute_check import YaMuteCheck
from update_mute_issues import (
    create_and_add_issue_to_project,
    generate_github_issue_title_and_body,
    get_muted_tests_from_issues,
    close_unmuted_issues,
)
from ydb_wrapper import YDBWrapper

from mute.logic import aggregate_test_data
from mute.apply import apply_and_add_mutes

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

script_dir = os.path.dirname(__file__)
repo_path = f"{script_dir}/../../../"
muted_ya_path = '.github/config/muted_ya.txt'

# Legacy hardcoded defaults
MUTE_DAYS = 4
UNMUTE_DAYS = 7
DELETE_DAYS = 7


def execute_query(branch='main', build_type='relwithdebinfo', days_window=1):
    today = datetime.date.today()
    start_date = today - datetime.timedelta(days=days_window - 1)
    logging.info(f"Executing query for branch='{branch}', build_type='{build_type}', days_window={days_window}")

    try:
        with YDBWrapper() as ydb_wrapper:
            if not ydb_wrapper.check_credentials():
                return []
            tests_monitor_table = ydb_wrapper.get_table_path("tests_monitor")
            query_string = f'''
    SELECT test_name, suite_folder, full_name, build_type, branch, date_window,
        pass_count, fail_count, mute_count, skip_count, success_rate, owner,
        is_muted, state, days_in_state, is_test_chunk
    FROM `{tests_monitor_table}`
    WHERE date_window >= CurrentUtcDate() - 7*Interval("P1D")
        AND branch = '{branch}' AND build_type = '{build_type}'
    '''
            results = ydb_wrapper.execute_scan_query(query_string, query_name=f"get_tests_monitor_data_{branch}")
            suite_test_names = {'unittest', 'py3test', 'gtest'}
            filtered = [r for r in results if r.get('test_name') and r.get('test_name') not in suite_test_names]
            logging.info(f"Query returned {len(results)}, after filtering: {len(filtered)}")
            return filtered
    except Exception as e:
        logging.error(f"Error executing query: {e}")
        raise


def _aggregate_with_logging(all_data, period_days):
    logging.info(f"Aggregating for {period_days} days...")
    result = aggregate_test_data(all_data, period_days)
    logging.info(f"Aggregation: {len(result)} unique tests")
    return result


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


def create_mute_issues(all_tests, file_path, close_issues=True):
    tests_from_file = read_tests_from_file(file_path)
    muted_tests_in_issues = get_muted_tests_from_issues()
    prepared_tests_by_suite = {}
    temp_tests_by_suite = {}
    muted_tests_set = {test['full_name'] for test in tests_from_file}

    closed_issues = []
    partially_unmuted_issues = []
    if close_issues:
        closed_issues, partially_unmuted_issues = close_unmuted_issues(muted_tests_set)

    for test in all_tests:
        for test_from_file in tests_from_file:
            if test['full_name'] == test_from_file['full_name']:
                if test['full_name'] in muted_tests_in_issues:
                    logging.info(f"test {test['full_name']} already have issue")
                else:
                    key = f"{test_from_file['testsuite']}:{test['owner']}"
                    if not temp_tests_by_suite.get(key):
                        temp_tests_by_suite[key] = []
                    temp_tests_by_suite[key].append({
                        'mute_string': f"{test.get('suite_folder')} {test.get('test_name')}",
                        'test_name': test.get('test_name'),
                        'suite_folder': test.get('suite_folder'),
                        'full_name': test.get('full_name'),
                        'success_rate': test.get('success_rate'),
                        'days_in_state': test.get('days_in_state'),
                        'date_window': test.get('date_window', 'N/A'),
                        'owner': test.get('owner'),
                        'state': test.get('state'),
                        'summary': test.get('summary'),
                        'fail_count': test.get('fail_count'),
                        'pass_count': test.get('pass_count'),
                        'branch': test.get('branch'),
                    })
                break

    for key, tests in temp_tests_by_suite.items():
        if len(tests) <= 40:
            prepared_tests_by_suite[key] = tests
        else:
            for i in range(0, len(tests), 40):
                chunk = tests[i:i + 40]
                chunk_key = f"{key}_{i // 40 + 1}"
                prepared_tests_by_suite[chunk_key] = chunk

    results = []
    for item in prepared_tests_by_suite:
        title, body = generate_github_issue_title_and_body(prepared_tests_by_suite[item])
        owner_value = prepared_tests_by_suite[item][0]['owner'].split('/', 1)[1] if '/' in prepared_tests_by_suite[item][0]['owner'] else prepared_tests_by_suite[item][0]['owner']
        result = create_and_add_issue_to_project(title, body, state='Muted', owner=owner_value)
        if not result:
            break
        results.append({'message': f"Created issue '{title}'", 'owner': owner_value})

    results.sort(key=lambda x: x['owner'])
    formatted = []
    if closed_issues:
        formatted.append("🔒 **CLOSED ISSUES**")
        for issue in closed_issues:
            formatted.append(f"✅ **Closed** {issue['url']}")
    if partially_unmuted_issues:
        formatted.append("🔓 **PARTIALLY UNMUTED ISSUES**")
        for issue in partially_unmuted_issues:
            formatted.append(f"⚠️ **Partially unmuted** {issue['url']}")
    if results:
        formatted.append("🆕 **CREATED ISSUES**")
        for r in results:
            formatted.append(f"   {r['message']}")

    print("\n".join(formatted))
    if 'GITHUB_OUTPUT' in os.environ and 'GITHUB_WORKSPACE' in os.environ:
        file_path_out = os.path.join(os.environ['GITHUB_WORKSPACE'], "created_issues.txt")
        with open(file_path_out, 'w') as f:
            f.write("\n".join(formatted) + "\n")
        with open(os.environ['GITHUB_OUTPUT'], 'a') as gh_out:
            gh_out.write(f"created_issues_file={file_path_out}")
    return results


def mute_worker(args):
    with YDBWrapper() as ydb_wrapper:
        if not ydb_wrapper.check_credentials():
            return 1

        logging.info(f"Starting legacy mute worker, branch: {args.branch}")
        input_muted_ya_path = getattr(args, 'muted_ya_file', muted_ya_path)
        mute_check = YaMuteCheck()
        mute_check.load(input_muted_ya_path)
        logging.info(f"Loaded muted_ya with {len(mute_check.regexps)} patterns")

        # Legacy: hardcoded params, no quarantine
        mute_rule_params = {'min_failures_high': 3, 'min_failures_low': 2, 'min_runs_threshold': 10, 'window_days': MUTE_DAYS}
        unmute_rule_params = {'min_runs': 4, 'max_fails': 0, 'window_days': UNMUTE_DAYS}
        delete_rule_params = {'window_days': DELETE_DAYS}

        build_type = getattr(args, 'build_type', 'relwithdebinfo')
        all_data = execute_query(args.branch, build_type=build_type, days_window=7)
        logging.info(f"Query returned {len(all_data)} test records")

        aggregated_for_mute = _aggregate_with_logging(all_data, MUTE_DAYS)
        aggregated_for_unmute = _aggregate_with_logging(all_data, UNMUTE_DAYS)
        aggregated_for_delete = _aggregate_with_logging(all_data, DELETE_DAYS)

        if args.mode == 'update_muted_ya':
            output_path = args.output_folder
            os.makedirs(output_path, exist_ok=True)
            result = apply_and_add_mutes(
                all_data, output_path, mute_check,
                aggregated_for_mute, aggregated_for_unmute, aggregated_for_delete,
                quarantine_check=None,
                to_graduated=set(),
                mute_rule_params=mute_rule_params,
                unmute_rule_params=unmute_rule_params,
                delete_rule_params=delete_rule_params,
                output_file=getattr(args, 'output_file', None),
            )
            # Legacy: no mute_decisions write
            if getattr(args, 'system_version', None):
                try:
                    from mute.decisions import write_mute_decisions  # noqa: E402
                    if ydb_wrapper.check_credentials():
                        write_mute_decisions(
                            ydb_wrapper, branch=args.branch, build_type=getattr(args, 'build_type', 'relwithdebinfo'),
                            to_mute=result[0], to_unmute=result[1], to_delete=result[2],
                            to_mute_debug=result[3], to_unmute_debug=result[4], to_delete_debug=result[5],
                            system_version=args.system_version,
                        )
                except Exception as e:
                    logging.warning(f"Failed to write mute decisions: {e}")

        elif args.mode == 'create_issues':
            create_mute_issues(aggregated_for_mute, args.file_path, close_issues=args.close_issues)

        logging.info("Mute worker completed")
        return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Legacy mute: tests_monitor, hardcoded rules")
    subparsers = parser.add_subparsers(dest='mode')

    update_parser = subparsers.add_parser('update_muted_ya')
    update_parser.add_argument('--output_folder', default=repo_path)
    update_parser.add_argument('--branch', default='main')
    update_parser.add_argument('--muted_ya_file', default=muted_ya_path)
    update_parser.add_argument('--quarantine_file', default='.github/config/quarantine.txt')
    update_parser.add_argument('--build_type', default='relwithdebinfo')
    update_parser.add_argument('--output_file', default=None)
    update_parser.add_argument('--legacy', action='store_true', help='No-op, legacy is always on')
    update_parser.add_argument('--system-version', '--system_version', dest='system_version', default=None)

    issues_parser = subparsers.add_parser('create_issues')
    issues_parser.add_argument('--file_path', default=f'{repo_path}/mute_update/to_mute.txt')
    issues_parser.add_argument('--branch', default='main')
    issues_parser.add_argument('--close_issues', action='store_true', default=True)

    args = parser.parse_args()
    if not args.mode:
        parser.print_help()
        sys.exit(1)
    sys.exit(mute_worker(args))
