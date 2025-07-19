#!/usr/bin/env python3
import argparse
import configparser
import datetime
import os
import re
import ydb
import logging
import sys
from pathlib import Path

# Add the parent directory to the path to import update_mute_issues
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

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


def execute_query(branch='main', build_type='relwithdebinfo', days_window=1):
    # Получаем today
    today = datetime.date.today()
    start_date = today - datetime.timedelta(days=days_window-1)
    end_date = today
    table_name = 'test_results/analytics/flaky_tests_window_1_days'
    owners_table = 'test_results/analytics/testowners'
    
    query_start_time = datetime.datetime.now()
    logging.info(f"Executing query for branch='{branch}', build_type='{build_type}', days_window={days_window}")
    logging.info(f"Date range: {start_date} to {end_date}")
    logging.info(f"Table: {table_name}")
    
    query_string = f'''
    SELECT
      t.test_name as test_name,
      t.suite_folder as suite_folder,
      t.full_name as full_name,
      t.build_type as build_type,
      t.branch as branch,
      SUM(t.pass_count) as pass_count,
      SUM(t.fail_count) as fail_count,
      SUM(t.mute_count) as mute_count,
      SUM(t.skip_count) as skip_count,
      o.owners as owner
    FROM `{table_name}` t
    LEFT JOIN `{owners_table}` o
      ON t.full_name = o.full_name
    WHERE t.date_window >= Date('{start_date}') AND t.date_window <= Date('{end_date}')
      AND t.branch = '{branch}' AND t.build_type = '{build_type}'
    GROUP BY t.test_name, t.suite_folder, t.full_name, t.build_type, t.branch, o.owners
    '''
    
    logging.info(f"SQL Query:\n{query_string}")
    
    try:
        with ydb.Driver(
            endpoint=DATABASE_ENDPOINT,
            database=DATABASE_PATH,
            credentials=ydb.credentials_from_env_variables(),
        ) as driver:
            driver.wait(timeout=10, fail_fast=True)
            logging.info("Successfully connected to YDB")
            
            query = ydb.ScanQuery(query_string, {})
            table_client = ydb.TableClient(driver, ydb.TableClientSettings())
            it = table_client.scan_query(query)
            results = []
            
            logging.info("Starting to fetch results...")
            row_count = 0
            while True:
                try:
                    result = next(it)
                    batch_results = result.result_set.rows
                    results.extend(batch_results)
                    row_count += len(batch_results)
                    logging.debug(f"Fetched batch of {len(batch_results)} rows, total: {row_count}")
                except StopIteration:
                    break
            
            query_end_time = datetime.datetime.now()
            query_duration = query_end_time - query_start_time
            logging.info(f"Query completed successfully. Total rows returned: {len(results)}")
            logging.info(f"Query execution time: {query_duration.total_seconds():.2f} seconds")
            return results
        
    except Exception as e:
        query_end_time = datetime.datetime.now()
        query_duration = query_end_time - query_start_time
        logging.error(f"Error executing query: {e}")
        logging.error(f"Query parameters: branch='{branch}', build_type='{build_type}', days_window={days_window}")
        logging.error(f"Date range: {start_date} to {end_date}")
        logging.error(f"Query execution time before error: {query_duration.total_seconds():.2f} seconds")
        raise


def add_lines_to_file(file_path, lines_to_add):
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'w') as f:
            f.writelines(lines_to_add)
        logging.info(f"Lines added to {file_path}")
    except Exception as e:
        logging.error(f"Error adding lines to {file_path}: {e}")

def calculate_success_rate(test):
    """Вычисляет success rate для теста"""
    runs = test.get('pass_count', 0) + test.get('fail_count', 0)
    return round((test.get('pass_count', 0) / runs * 100) if runs > 0 else 0, 1)

def create_test_string(test, use_wildcards=False):
    """Создает строку теста с опциональными wildcards"""
    testsuite = test.get('suite_folder')
    testcase = test.get('test_name')
    test_string = f"{testsuite} {testcase}\n"
    if use_wildcards:
        test_string = re.sub(r'\d+/(\d+)\]', r'*/*]', test_string)
    return test_string

def create_debug_string(test, resolution, success_rate=None):
    """Создает debug строку для теста"""
    if success_rate is None:
        success_rate = calculate_success_rate(test)
    
    testsuite = test.get('suite_folder')
    testcase = test.get('test_name')
    runs = test.get('pass_count', 0) + test.get('fail_count', 0)
    
    debug_string = f"{testsuite} {testcase} # owner {test.get('owner', 'N/A')} success_rate {success_rate}%"
    
    debug_string += f", p-{test.get('pass_count')}, f-{test.get('fail_count')},m-{test.get('mute_count')}, s-{test.get('skip_count')}, total {runs}, resolution {resolution}"
    
    return debug_string + "\n"

def is_flaky_test(test):
    """Проверяет, является ли тест flaky"""
    fails = test.get('fail_count', 0)
    runs = test.get('pass_count', 0) + fails
    return (fails >= 2) or (fails >= 1 and runs <= 10)

def is_unmute_candidate(test, unmute_stats):
    """Проверяет, является ли тест кандидатом на размьют"""
    if not unmute_stats:
        return False
    unmute_runs = unmute_stats.get('pass_count', 0) + unmute_stats.get('fail_count', 0) + unmute_stats.get('mute_count', 0)
    unmute_fails = unmute_stats.get('fail_count', 0) + unmute_stats.get('mute_count', 0)
    return unmute_runs > 4 and unmute_fails == 0

def is_delete_candidate(test, delete_stats):
    """Проверяет, является ли тест кандидатом на удаление из mute"""
    if not delete_stats:
        return False
    delete_runs = delete_stats.get('pass_count', 0) + delete_stats.get('fail_count', 0) + delete_stats.get('mute_count', 0) + delete_stats.get('skip_count', 0)
    return delete_runs == 0

def create_file_set(aggregated_for_mute, filter_func, mute_check=None, use_wildcards=False, resolution=None):
    """Создает набор тестов для файла на основе фильтра"""
    result_set = set()
    debug_list = []
    
    for test in aggregated_for_mute:
        testsuite = test.get('suite_folder')
        testcase = test.get('test_name')
        
        if not testsuite or not testcase:
            continue
            
        # Проверяем mute_check если передан
        if mute_check and not mute_check(testsuite, testcase):
            continue
            
        # Применяем фильтр
        if filter_func(test):
            test_string = create_test_string(test, use_wildcards)
            result_set.add(test_string)
            
            if resolution:
                debug_string = create_debug_string(test, resolution)
                debug_list.append(debug_string)
    
    return sorted(result_set), sorted(debug_list)

def write_file_set(file_path, test_set, debug_list=None):
    """Записывает набор тестов в файл"""
    add_lines_to_file(file_path, test_set)
    if debug_list:
        debug_path = file_path.replace('.txt', '_debug.txt')
        add_lines_to_file(debug_path, debug_list)
    logging.info(f"Created {os.path.basename(file_path)} with {len(test_set)} tests")

def apply_and_add_mutes(aggregated_for_mute, aggregated_for_unmute, aggregated_for_delete, output_path, mute_check):
    output_path = os.path.join(output_path, 'mute_update')
    logging.info(f"Creating mute files in directory: {output_path}")
    
    aggregated_for_mute = sorted(aggregated_for_mute, key=lambda d: d['full_name'])
    aggregated_for_unmute = {t['full_name']: t for t in aggregated_for_unmute}
    aggregated_for_delete = {t['full_name']: t for t in aggregated_for_delete}
    
    logging.info(f"Processing {len(aggregated_for_mute)} tests for mute analysis")

    try:
        # 1. Кандидаты на mute
        def is_mute_candidate(test):
            return is_flaky_test(test)
        
        to_mute, to_mute_debug = create_file_set(
            aggregated_for_mute, is_mute_candidate, use_wildcards=True, resolution='to_mute'
        )
        write_file_set(os.path.join(output_path, 'to_mute.txt'), to_mute, to_mute_debug)
        
        # 2. Кандидаты на размьют
        def is_unmute_candidate_wrapper(test):
            return is_unmute_candidate(test, aggregated_for_unmute.get(test.get('full_name')))
        
        to_unmute, to_unmute_debug = create_file_set(
            aggregated_for_mute, is_unmute_candidate_wrapper, mute_check, resolution='to_unmute'
        )
        write_file_set(os.path.join(output_path, 'to_unmute.txt'), to_unmute, to_unmute_debug)
        
        # 3. Кандидаты на удаление из mute (to_delete)
        def is_delete_candidate_wrapper(test):
            return is_delete_candidate(test, aggregated_for_delete.get(test.get('full_name')))
        
        to_delete, to_delete_debug = create_file_set(
            aggregated_for_mute, is_delete_candidate_wrapper, mute_check, resolution='to_delete'
        )
        write_file_set(os.path.join(output_path, 'to_delete.txt'), to_delete, to_delete_debug)
        
        # 4. muted_ya (все замьюченные сейчас)
        all_muted_ya, all_muted_ya_debug = create_file_set(
            aggregated_for_mute, lambda test: mute_check(test.get('suite_folder'), test.get('test_name')) if mute_check else True, use_wildcards=True, resolution='muted_ya'
        )
        write_file_set(os.path.join(output_path, 'muted_ya.txt'), all_muted_ya, all_muted_ya_debug)
        to_mute_set = set(to_mute)
        to_unmute_set = set(to_unmute)
        to_delete_set = set(to_delete)
        all_muted_ya_set = set(all_muted_ya)
        
         # Создаем словари для быстрого поиска debug-строк
        all_muted_ya_debug_dict = dict(zip(all_muted_ya, all_muted_ya_debug))
        to_mute_debug_dict = dict(zip(to_mute, to_mute_debug))
        
        # 5. muted_ya+to_mute
        muted_ya_plus_to_mute = sorted(list(all_muted_ya_set | to_mute_set))
        muted_ya_plus_to_mute_debug = []
        for test in muted_ya_plus_to_mute:
            if test in all_muted_ya_debug_dict:
                muted_ya_plus_to_mute_debug.append(all_muted_ya_debug_dict[test])
            elif test in to_mute_debug_dict:
                muted_ya_plus_to_mute_debug.append(to_mute_debug_dict[test])
        write_file_set(os.path.join(output_path, 'muted_ya+to_mute.txt'), muted_ya_plus_to_mute, muted_ya_plus_to_mute_debug)
        
        # 6. muted_ya-to_unmute
        muted_ya_minus_to_unmute = [t for t in all_muted_ya if t not in to_unmute_set]
        muted_ya_minus_to_unmute_debug = [all_muted_ya_debug_dict[t] for t in muted_ya_minus_to_unmute if t in all_muted_ya_debug_dict]
        write_file_set(os.path.join(output_path, 'muted_ya-to_unmute.txt'), muted_ya_minus_to_unmute, muted_ya_minus_to_unmute_debug)
        
        # 7. muted_ya-to_delete
        muted_ya_minus_to_delete = [t for t in all_muted_ya if t not in to_delete_set]
        muted_ya_minus_to_delete_debug = [all_muted_ya_debug_dict[t] for t in muted_ya_minus_to_delete if t in all_muted_ya_debug_dict]
        write_file_set(os.path.join(output_path, 'muted_ya-to_delete.txt'), muted_ya_minus_to_delete, muted_ya_minus_to_delete_debug)
        
        # 8. muted_ya-to-delete-to-unmute
        muted_ya_minus_to_delete_to_unmute = [t for t in all_muted_ya if t not in to_delete_set and t not in to_unmute_set]
        muted_ya_minus_to_delete_to_unmute_debug = [all_muted_ya_debug_dict[t] for t in muted_ya_minus_to_delete_to_unmute if t in all_muted_ya_debug_dict]
        write_file_set(os.path.join(output_path, 'muted_ya-to-delete-to-unmute.txt'), muted_ya_minus_to_delete_to_unmute, muted_ya_minus_to_delete_to_unmute_debug)
        
        # 9. muted_ya-to-delete-to-unmute+to_mute
        muted_ya_minus_to_delete_to_unmute_set = set(muted_ya_minus_to_delete_to_unmute)
        muted_ya_minus_to_delete_to_unmute_plus_to_mute = sorted(list(muted_ya_minus_to_delete_to_unmute_set | to_mute_set))
        muted_ya_minus_to_delete_to_unmute_plus_to_mute_debug = []
        for test in muted_ya_minus_to_delete_to_unmute_plus_to_mute:
            if test in muted_ya_minus_to_delete_to_unmute_set and test in all_muted_ya_debug_dict:
                muted_ya_minus_to_delete_to_unmute_plus_to_mute_debug.append(all_muted_ya_debug_dict[test])
            elif test in to_mute_debug_dict:
                muted_ya_minus_to_delete_to_unmute_plus_to_mute_debug.append(to_mute_debug_dict[test])
        write_file_set(os.path.join(output_path, 'muted_ya-to-delete-to-unmute+to_mute.txt'), muted_ya_minus_to_delete_to_unmute_plus_to_mute, muted_ya_minus_to_delete_to_unmute_plus_to_mute_debug)
        
        # Логирование итоговых результатов
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

    logging.info(f"Starting mute worker with mode: {args.mode}")
    logging.info(f"Branch: {args.branch}")
    
    mute_check = YaMuteCheck()
    mute_check.load(muted_ya_path)
    logging.info(f"Loaded muted_ya.txt with {len(mute_check.regexps)} test patterns")

    logging.info("Executing queries for different time windows...")
    
    logging.info("Executing query for mute candidates (3 days)...")
    aggregated_for_mute = execute_query(args.branch, days_window=3)
    
    logging.info("Executing query for unmute candidates (2 days)...")
    aggregated_for_unmute = execute_query(args.branch, days_window=2)
    
    logging.info("Executing query for delete candidates (7 days)...")
    aggregated_for_delete = execute_query(args.branch, days_window=7)
    
    logging.info(f"Query results: mute={len(aggregated_for_mute)}, unmute={len(aggregated_for_unmute)}, delete={len(aggregated_for_delete)}")
    
    if args.mode == 'update_muted_ya':
        output_path = args.output_folder
        os.makedirs(output_path, exist_ok=True)
        logging.info(f"Creating mute files in: {output_path}")
        apply_and_add_mutes(aggregated_for_mute, aggregated_for_unmute, aggregated_for_delete, output_path, mute_check)

    elif args.mode == 'create_issues':
        file_path = args.file_path
        logging.info(f"Creating issues from file: {file_path}")
        create_mute_issues(aggregated_for_mute, file_path, close_issues=args.close_issues)
    
    logging.info("Mute worker completed successfully")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Add tests to mutes files based on flaky_today condition")

    subparsers = parser.add_subparsers(dest='mode', help="Mode to perform")

    update_muted_ya_parser = subparsers.add_parser('update_muted_ya', help='create new muted_ya')
    update_muted_ya_parser.add_argument('--output_folder', default=repo_path, required=False, help='Output folder.')
    update_muted_ya_parser.add_argument('--branch', default='main', help='Branch to get history')

    create_issues_parser = subparsers.add_parser(
        'create_issues',
        help='create issues by muted_ya like files',
    )
    create_issues_parser.add_argument(
        '--file_path', default=f'{repo_path}/mute_update/flaky.txt', required=False, help='file path'
    )
    create_issues_parser.add_argument('--branch', default='main', help='Branch to get history')
    create_issues_parser.add_argument('--close_issues', action='store_true', default=True, help='Close issues when all tests are unmuted (default: True)')

    args = parser.parse_args()

    mute_worker(args)
