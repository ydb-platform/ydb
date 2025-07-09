#!/usr/bin/env python3
import argparse
import configparser
import datetime
import os
import re
import ydb
import logging

from transform_ya_junit import YaMuteCheck
from update_mute_issues import (
    create_and_add_issue_to_project,
    generate_github_issue_title_and_body,
    get_muted_tests_from_issues,
    close_unmuted_issues,
)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

dir = os.path.dirname(__file__)
config = configparser.ConfigParser()
config_file_path = f"{dir}/../../config/ydb_qa_db.ini"
repo_path = f"{dir}/../../../"
muted_ya_path = '.github/config/muted_ya.txt'
config.read(config_file_path)

DATABASE_ENDPOINT = config["QA_DB"]["DATABASE_ENDPOINT"]
DATABASE_PATH = config["QA_DB"]["DATABASE_PATH"]


def execute_query(driver, branch='main', build_type='relwithdebinfo', days_back=1):
    # Handle days_back=0 case - don't add date filter for flaky tests
    date_filter = "AND date_window >= CurrentUtcDate()" if days_back == 0 else f"AND date_window >= CurrentUtcDate() - {days_back}*Interval(\"P1D\")"
    
    query_string = f'''
    SELECT * from (
        SELECT data.*,
        CASE WHEN new_flaky.full_name IS NOT NULL THEN True ELSE False END AS new_flaky_today,
        CASE WHEN flaky.full_name IS NOT NULL THEN True ELSE False END AS flaky_today,
        CASE WHEN muted_stable.full_name IS NOT NULL THEN True ELSE False END AS muted_stable_today,
        CASE WHEN muted_stable_n_days.full_name IS NOT NULL THEN True ELSE False END AS muted_stable_n_days_today,
        CASE WHEN deleted.full_name IS NOT NULL THEN True ELSE False END AS deleted_today

        FROM
        (SELECT test_name, suite_folder, full_name, date_window, build_type, branch, days_ago_window, history, history_class, pass_count, mute_count, fail_count, skip_count, success_rate, summary, owner, is_muted, is_test_chunk, state, previous_state, state_change_date, days_in_state, previous_state_filtered, state_change_date_filtered, days_in_state_filtered, state_filtered
        FROM `test_results/analytics/tests_monitor`) as data
        left JOIN 
        (SELECT full_name, build_type, branch
            FROM `test_results/analytics/tests_monitor`
            WHERE state = 'Flaky'
            AND days_in_state <= 2
            {date_filter}
            )as new_flaky
        ON 
            data.full_name = new_flaky.full_name
            and data.build_type = new_flaky.build_type
            and data.branch = new_flaky.branch
        LEFT JOIN 
        (SELECT full_name, build_type, branch
            FROM `test_results/analytics/tests_monitor`
            WHERE state = 'Flaky'
            {date_filter}
            )as flaky
        ON 
            data.full_name = flaky.full_name
            and data.build_type = flaky.build_type
            and data.branch = flaky.branch
        LEFT JOIN 
        (SELECT full_name, build_type, branch
            FROM `test_results/analytics/tests_monitor`
            WHERE state = 'Muted Stable'
            AND date_window = CurrentUtcDate()
            )as muted_stable

        ON 
            data.full_name = muted_stable.full_name
            and data.build_type = muted_stable.build_type
            and data.branch = muted_stable.branch
        LEFT JOIN 
        (SELECT full_name, build_type, branch
            FROM `test_results/analytics/tests_monitor`
            WHERE state= 'Muted Stable'
            AND days_in_state >= 5
            AND date_window = CurrentUtcDate()
            and is_test_chunk = 0
            )as muted_stable_n_days

        ON 
            data.full_name = muted_stable_n_days.full_name
            and data.build_type = muted_stable_n_days.build_type
            and data.branch = muted_stable_n_days.branch
       
        LEFT JOIN 
        (SELECT full_name, build_type, branch
            FROM `test_results/analytics/tests_monitor`
            WHERE state = 'no_runs'
            AND days_in_state >= 10
            AND date_window = CurrentUtcDate()
            and is_test_chunk = 0
            )as deleted

        ON 
            data.full_name = deleted.full_name
            and data.build_type = deleted.build_type
            and data.branch = deleted.branch
        ) 
        where date_window = CurrentUtcDate() and branch = '{branch}' and build_type = '{build_type}'
    
    '''

    query = ydb.ScanQuery(query_string, {})
    table_client = ydb.TableClient(driver, ydb.TableClientSettings())
    it = table_client.scan_query(query)
    results = []
    while True:
        try:
            result = next(it)
            results = results + result.result_set.rows
        except StopIteration:
            break

    return results


def add_lines_to_file(file_path, lines_to_add):
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'w') as f:
            f.writelines(lines_to_add)
        logging.info(f"Lines added to {file_path}")
    except Exception as e:
        logging.error(f"Error adding lines to {file_path}: {e}")


def apply_and_add_mutes(all_tests, output_path, mute_check, days_back=1):

    output_path = os.path.join(output_path, 'mute_update')
    
    # Add base date for date_window conversion
    base_date = datetime.datetime(1970, 1, 1)
    
    # Calculate period info for debug strings
    if days_back == 0:
        period_info = "period today"
    else:
        period_info = f"period {days_back + 1}d"

    all_tests = sorted(all_tests, key=lambda d: d['full_name'])

    try:

        deleted_tests = set(
            f"{test.get('suite_folder')} {test.get('test_name')}\n" for test in all_tests if test.get('deleted_today')
        )

        deleted_tests = sorted(deleted_tests)
        add_lines_to_file(os.path.join(output_path, 'deleted.txt'), deleted_tests)

        deleted_tests_debug = set(
            f"{test.get('suite_folder')} {test.get('test_name')} # owner {test.get('owner')} success_rate {test.get('success_rate')}% runs {(test.get('pass_count') or 0) + (test.get('fail_count') or 0) + (test.get('mute_count') or 0) + (test.get('skip_count') or 0)} ({test.get('pass_count') or 0}p/{test.get('fail_count') or 0}f/{test.get('mute_count') or 0}m/{test.get('skip_count') or 0}s), state {test.get('state')} days in state {test.get('days_in_state')}, {period_info}, snapshot_date {(base_date + datetime.timedelta(days=test.get('date_window'))).date()}\n"
            for test in all_tests
            if test.get('deleted_today')
        )

        deleted_tests_debug = sorted(deleted_tests_debug)
        add_lines_to_file(os.path.join(output_path, 'deleted_debug.txt'), deleted_tests_debug)

        muted_stable_tests = set(
            f"{test.get('suite_folder')} {test.get('test_name')}\n"
            for test in all_tests
            if test.get('muted_stable_n_days_today')
        )

        muted_stable_tests = sorted(muted_stable_tests)
        add_lines_to_file(os.path.join(output_path, 'muted_stable.txt'), muted_stable_tests)

        muted_stable_tests_debug = set(
            f"{test.get('suite_folder')} {test.get('test_name')} "
            + f"# owner {test.get('owner')} success_rate {test.get('success_rate')}% runs {(test.get('pass_count') or 0) + (test.get('fail_count') or 0) + (test.get('mute_count') or 0) + (test.get('skip_count') or 0)} ({test.get('pass_count') or 0}p/{test.get('fail_count') or 0}f/{test.get('mute_count') or 0}m/{test.get('skip_count') or 0}s), state {test.get('state')} days in state {test.get('days_in_state')}, {period_info}, snapshot_date {(base_date + datetime.timedelta(days=test.get('date_window'))).date()}\n"
            for test in all_tests
            if test.get('muted_stable_n_days_today')
        )

        muted_stable_tests_debug = sorted(muted_stable_tests_debug)
        add_lines_to_file(os.path.join(output_path, 'muted_stable_debug.txt'), muted_stable_tests_debug)

        # Add all flaky tests
        flaky_tests = set(
            re.sub(r'\d+/(\d+)\]', r'*/*]', f"{test.get('suite_folder')} {test.get('test_name')}\n")
            for test in all_tests
            if test.get('flaky_today')
            and test.get('fail_count') >= 2
        )
        flaky_tests = sorted(flaky_tests)
        add_lines_to_file(os.path.join(output_path, 'flaky.txt'), flaky_tests)

        flaky_tests_debug = set(
            re.sub(r'\d+/(\d+)\]', r'*/*]', f"{test.get('suite_folder')} {test.get('test_name')}")
            + f" # owner {test.get('owner')} success_rate {test.get('success_rate')}% runs {(test.get('pass_count') or 0) + (test.get('fail_count') or 0) + (test.get('mute_count') or 0) + (test.get('skip_count') or 0)} ({test.get('pass_count') or 0}p/{test.get('fail_count') or 0}f/{test.get('mute_count') or 0}m/{test.get('skip_count') or 0}s), state {test.get('state')}, days in state {test.get('days_in_state')}, {period_info}, snapshot_date {(base_date + datetime.timedelta(days=test.get('date_window'))).date()}\n"
            for test in all_tests
            if test.get('flaky_today')
            and test.get('fail_count') >= 2
        )
        ## тесты может запускаться 1 раз в день. если за последние 7 дней набирается трешход то мьютим
        ## падения сегодня более весомы ??  
        ## за 7 дней смотреть?
        #----
        ## Mute Flaky редко запускаемых тестов
        ##   Разобраться почему 90 % флакающих тестов имеют только 1 падение и в статусе Flaky только 2 дня      
        flaky_tests_debug = sorted(flaky_tests_debug)
        add_lines_to_file(os.path.join(output_path, 'flaky_debug.txt'), flaky_tests_debug)

        new_muted_ya_tests_debug = []
        new_muted_ya_tests = []
        new_muted_ya_tests_with_flaky = []
        new_muted_ya_tests_with_flaky_debug = []
        unmuted_tests_debug = []
        muted_ya_tests_sorted = []
        muted_ya_tests_sorted_debug = []
        deleted_tests_in_mute = []
        deleted_tests_in_mute_debug = []
        muted_before_count = 0
        unmuted_stable = 0
        unmuted_deleted = 0
        # Apply mute check and filter out already muted tests
        for test in all_tests:
            testsuite = test.get('suite_folder')
            testcase = test.get('test_name')
            success_rate = test.get('success_rate')
            days_in_state = test.get('days_in_state')
            owner = test.get('owner')
            state = test.get('state')
            pass_count = test.get('pass_count')
            fail_count = test.get('fail_count')
            mute_count = test.get('mute_count')
            skip_count = test.get('skip_count')
            date_window = test.get('date_window')
            
            test_string = f"{testsuite} {testcase}\n"
            test_string_debug = f"{testsuite} {testcase} # owner {owner} success_rate {success_rate}% runs {(pass_count or 0) + (fail_count or 0) + (mute_count or 0) + (skip_count or 0)} ({pass_count or 0}p/{fail_count or 0}f/{mute_count or 0}m/{skip_count or 0}s), state {state} days in state {days_in_state}, {period_info}, snapshot_date {(base_date + datetime.timedelta(days=date_window)).date()}\n"
            test_string = re.sub(r'\d+/(\d+)\]', r'*/*]', test_string)
            if (
                testsuite and testcase and mute_check(testsuite, testcase) or test_string in flaky_tests
            ) and test_string not in new_muted_ya_tests_with_flaky:
                if test_string not in muted_stable_tests and test_string not in deleted_tests:
                    new_muted_ya_tests_with_flaky.append(test_string)
                    new_muted_ya_tests_with_flaky_debug.append(test_string_debug)

            if testsuite and testcase and mute_check(testsuite, testcase):
               
                if test_string not in muted_ya_tests_sorted:
                    muted_ya_tests_sorted.append(test_string)
                    muted_ya_tests_sorted_debug.append(test_string_debug)
                    muted_before_count += 1
                if test_string not in new_muted_ya_tests:
                    if test_string not in muted_stable_tests and test_string not in deleted_tests:
                        new_muted_ya_tests.append(test_string)
                        new_muted_ya_tests_debug.append(test_string_debug)
                    if test_string in muted_stable_tests:
                        unmuted_stable += 1
                    if test_string in deleted_tests:
                        unmuted_deleted += 1
                        deleted_tests_in_mute.append(test_string)
                        deleted_tests_in_mute_debug.append(test_string_debug)
                    unmuted_tests_debug.append(test_string_debug)

        muted_ya_tests_sorted = sorted(muted_ya_tests_sorted)
        add_lines_to_file(os.path.join(output_path, 'muted_ya_sorted.txt'), muted_ya_tests_sorted)
        muted_ya_tests_sorted_debug = sorted(muted_ya_tests_sorted_debug)
        add_lines_to_file(os.path.join(output_path, 'muted_ya_sorted_debug.txt'), muted_ya_tests_sorted_debug)
        new_muted_ya_tests = sorted(new_muted_ya_tests)
        add_lines_to_file(os.path.join(output_path, 'new_muted_ya.txt'), new_muted_ya_tests)
        new_muted_ya_tests_debug = sorted(new_muted_ya_tests_debug)
        add_lines_to_file(os.path.join(output_path, 'new_muted_ya_debug.txt'), new_muted_ya_tests_debug)
        new_muted_ya_tests_with_flaky = sorted(new_muted_ya_tests_with_flaky)
        add_lines_to_file(os.path.join(output_path, 'new_muted_ya_with_flaky.txt'), new_muted_ya_tests_with_flaky)
        new_muted_ya_tests_with_flaky_debug = sorted(new_muted_ya_tests_with_flaky_debug)
        add_lines_to_file(
            os.path.join(output_path, 'new_muted_ya_with_flaky_debug.txt'), new_muted_ya_tests_with_flaky_debug
        )
        unmuted_tests_debug = sorted(unmuted_tests_debug)
        add_lines_to_file(os.path.join(output_path, 'unmuted_debug.txt'), unmuted_tests_debug)
        deleted_tests_in_mute = sorted(deleted_tests_in_mute)
        add_lines_to_file(os.path.join(output_path, 'deleted_tests_in_mute.txt'), deleted_tests_in_mute)
        deleted_tests_in_mute_debug = sorted(deleted_tests_in_mute_debug)
        add_lines_to_file(os.path.join(output_path, 'deleted_tests_in_mute_debug.txt'), deleted_tests_in_mute_debug)

        logging.info(f"Muted before script: {muted_before_count} tests")
        logging.info(f"Muted stable : {len(muted_stable_tests)}")
        logging.info(f"Flaky tests : {len(flaky_tests)}")
        logging.info(f"Result: Muted without deleted and stable : {len(new_muted_ya_tests)}")
        logging.info(f"Result: Muted without deleted and stable, with flaky : {len(new_muted_ya_tests_with_flaky)}")
        logging.info(f"Result: Unmuted tests : stable {unmuted_stable} and deleted {unmuted_deleted}")
    except (KeyError, TypeError) as e:
        logging.error(f"Error processing test data: {e}. Check your query results for valid keys.")
        return 0

    return len(new_muted_ya_tests)


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
    base_date = datetime.datetime(1970, 1, 1)
    tests_from_file = read_tests_from_file(file_path)
    muted_tests_in_issues = get_muted_tests_from_issues()
    prepared_tests_by_suite = {}
    temp_tests_by_suite = {}
    
    # Create set of muted tests for faster lookup
    muted_tests_set = {test['full_name'] for test in tests_from_file}
    
    # Check and close issues if needed
    closed_issues = []
    partially_unmuted_issues = []
    if close_issues:
        closed_issues, partially_unmuted_issues = close_unmuted_issues(muted_tests_set)
    
    # First, collect all tests into temporary dictionary
    for test in all_tests:
        for test_from_file in tests_from_file:
            if test['full_name'] == test_from_file['full_name']:
                if test['full_name'] in muted_tests_in_issues:
                    logging.info(
                        f"test {test['full_name']} already have issue, {muted_tests_in_issues[test['full_name']][0]['url']}"
                    )
                else:
                    key = f"{test_from_file['testsuite']}:{test['owner']}"
                    if not temp_tests_by_suite.get(key):
                        temp_tests_by_suite[key] = []
                    temp_tests_by_suite[key].append(
                        {
                            'mute_string': f"{ test.get('suite_folder')} {test.get('test_name')}",
                            'test_name': test.get('test_name'),
                            'suite_folder': test.get('suite_folder'),
                            'full_name': test.get('full_name'),
                            'success_rate': test.get('success_rate'),
                            'days_in_state': test.get('days_in_state'),
                            'date_window': (base_date + datetime.timedelta(days=test.get('date_window'))).date() ,
                            'owner': test.get('owner'),
                            'state': test.get('state'),
                            'summary': test.get('summary'),
                            'fail_count': test.get('fail_count'),
                            'pass_count': test.get('pass_count'),
                            'branch': test.get('branch'),
                        }
                    )
    
    # Split groups larger than 20 tests
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
    for item in prepared_tests_by_suite:
        title, body = generate_github_issue_title_and_body(prepared_tests_by_suite[item])
        owner_value = prepared_tests_by_suite[item][0]['owner'].split('/', 1)[1] if '/' in prepared_tests_by_suite[item][0]['owner'] else prepared_tests_by_suite[item][0]['owner']
        result = create_and_add_issue_to_project(title, body, state='Muted', owner=owner_value)
        if not result:
            break
        else:
            results.append(
                {
                    'message': f"Created issue '{title}' for TEAM:@ydb-platform/{owner_value}, url {result['issue_url']}",
                    'owner': owner_value
                }
            )

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
            formatted_results.append("   🔒 **Still muted tests:**")
            for test in issue['still_muted_tests']:
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
            gh_out.write(f"created_issues_file={file_path}")
            
        print(f"Result saved to env variable GITHUB_OUTPUT by key created_issues_file")


def mute_worker(args):

    # Simplified Connection
    if "CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS" not in os.environ:
        print("Error: Env variable CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS is missing, skipping")
        return 1
    else:
        # Do not set up 'real' variable from gh workflows because it interfere with ydb tests
        # So, set up it locally
        os.environ["YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"] = os.environ[
            "CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"
        ]

    mute_check = YaMuteCheck()
    mute_check.load(muted_ya_path)

    with ydb.Driver(
        endpoint=DATABASE_ENDPOINT,
        database=DATABASE_PATH,
        credentials=ydb.credentials_from_env_variables(),
    ) as driver:
        driver.wait(timeout=10, fail_fast=True)

        all_tests = execute_query(driver, args.branch, args.build_type, args.days_back)
    if args.mode == 'update_muted_ya':
        output_path = args.output_folder
        os.makedirs(output_path, exist_ok=True)
        apply_and_add_mutes(all_tests, output_path, mute_check, args.days_back)

    elif args.mode == 'create_issues':
        file_path = args.file_path
        create_mute_issues(all_tests, file_path, close_issues=args.close_issues)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Add tests to mutes files based on flaky_today condition")

    subparsers = parser.add_subparsers(dest='mode', help="Mode to perform")

    update_muted_ya_parser = subparsers.add_parser('update_muted_ya', help='create new muted_ya')
    update_muted_ya_parser.add_argument('--output_folder', default=repo_path, required=False, help='Output folder.')
    update_muted_ya_parser.add_argument('--branch', default='main', help='Branch to get history')
    update_muted_ya_parser.add_argument('--build_type', default='relwithdebinfo', help='Build type to get history')
    update_muted_ya_parser.add_argument('--days_back', type=int, default=1, help='Number of days to look back for flaky tests')

    create_issues_parser = subparsers.add_parser(
        'create_issues',
        help='create issues by muted_ya like files',
    )
    create_issues_parser.add_argument(
        '--file_path', default=f'{repo_path}/mute_update/flaky.txt', required=False, help='file path'
    )
    create_issues_parser.add_argument('--branch', default='main', help='Branch to get history')
    create_issues_parser.add_argument('--build_type', default='relwithdebinfo', help='Build type to get history')
    create_issues_parser.add_argument('--days_back', type=int, default=1, help='Number of days to look back for flaky tests')
    create_issues_parser.add_argument('--close_issues', action='store_true', default=True, help='Close issues when all tests are unmuted (default: True)')

    args = parser.parse_args()

    mute_worker(args)
