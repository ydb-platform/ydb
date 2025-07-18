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
    
    query_start_time = datetime.datetime.now()
    logging.info(f"Executing query for branch='{branch}', build_type='{build_type}', days_window={days_window}")
    logging.info(f"Date range: {start_date} to {end_date}")
    logging.info(f"Table: {table_name}")
    
    query_string = f'''
    SELECT
      test_name,
      suite_folder,
      full_name,
      build_type,
      branch,
      SUM(pass_count) as pass_count,
      SUM(fail_count) as fail_count,
      MAX(owners) as owner
    FROM `{table_name}`
    WHERE date_window >= Date('{start_date}') AND date_window <= Date('{end_date}')
      AND branch = '{branch}' AND build_type = '{build_type}'
    GROUP BY test_name, suite_folder, full_name, build_type, branch
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


def apply_and_add_mutes(aggregated_for_mute, aggregated_for_unmute, aggregated_for_delete, output_path, mute_check):
    output_path = os.path.join(output_path, 'mute_update')
    logging.info(f"Creating mute files in directory: {output_path}")
    
    aggregated_for_mute = sorted(aggregated_for_mute, key=lambda d: d['full_name'])
    aggregated_for_unmute = {t['full_name']: t for t in aggregated_for_unmute}
    aggregated_for_delete = {t['full_name']: t for t in aggregated_for_delete}
    
    logging.info(f"Processing {len(aggregated_for_mute)} tests for mute analysis")

    try:
        # Определяем категории тестов для дополнительных файлов
        deleted_tests = set(
            f"{test.get('suite_folder')} {test.get('test_name')}\n" for test in aggregated_for_mute if test.get('fail_count', 0) == 0 and test.get('pass_count', 0) == 0
        )
        
        muted_stable_tests = set(
            f"{test.get('suite_folder')} {test.get('test_name')}\n"
            for test in aggregated_for_mute
            if test.get('muted_stable_n_days_today')
        )
        
        flaky_tests = set(
            re.sub(r'\d+/(\d+)\]', r'*/*]', f"{test.get('suite_folder')} {test.get('test_name')}\n")
            for test in aggregated_for_mute
            if (test.get('fail_count', 0) >= 2) or (test.get('fail_count', 0) >= 1 and (test.get('pass_count', 0) + test.get('fail_count', 0)) <= 10)
        )

        # Кандидаты на mute (по новым правилам)
        muted_candidates = set()
        muted_candidates_debug = []
        for test in aggregated_for_mute:
            runs = test.get('pass_count', 0) + test.get('fail_count', 0)
            fails = test.get('fail_count', 0)
            test_string = f"{test.get('suite_folder')} {test.get('test_name')}\n"
            # Вычисляем success_rate
            success_rate = round((test.get('pass_count', 0) / runs * 100) if runs > 0 else 0, 1)
            test_string_debug = f"{test.get('suite_folder')} {test.get('test_name')} # owner {test.get('owner', 'N/A')} success_rate {success_rate}%, days in state N/A, pass_count {test.get('pass_count')}, fail_count {test.get('fail_count')}, total_runs {runs}, resolution to_mute\n"
            if (fails >= 2) or (fails >= 1 and runs <= 10):
                muted_candidates.add(test_string)
                muted_candidates_debug.append(test_string_debug)
        muted_candidates = sorted(muted_candidates)
        muted_candidates_debug = sorted(muted_candidates_debug)
        add_lines_to_file(os.path.join(output_path, 'to_mute.txt'), muted_candidates)
        add_lines_to_file(os.path.join(output_path, 'to_mute_debug.txt'), muted_candidates_debug)
        logging.info(f"Created to_mute.txt with {len(muted_candidates)} candidates")
        logging.info(f"Created to_mute_debug.txt with {len(muted_candidates_debug)} candidates")

        # Кандидаты на размьют (по новым правилам: за 2 дня >4 запусков и нет падений)
        unmuted_candidates = set()
        unmuted_candidates_debug = []
        for test in aggregated_for_mute:
            testsuite = test.get('suite_folder')
            testcase = test.get('test_name')
            test_string = f"{testsuite} {testcase}\n"
            # Проверяем только тесты, которые уже в muted_ya
            if testsuite and testcase and mute_check(testsuite, testcase):
                unmute_stats = aggregated_for_unmute.get(test.get('full_name'))
                if unmute_stats:
                    unmute_runs = unmute_stats.get('pass_count', 0) + unmute_stats.get('fail_count', 0)
                    unmute_fails = unmute_stats.get('fail_count', 0)
                    if unmute_runs > 4 and unmute_fails == 0:
                        # Вычисляем success_rate для unmute
                        unmute_total_runs = test.get('pass_count', 0) + test.get('fail_count', 0)
                        unmute_success_rate = round((test.get('pass_count', 0) / unmute_total_runs * 100) if unmute_total_runs > 0 else 0, 1)
                        test_string_debug = f"{testsuite} {testcase} # owner {test.get('owner', 'N/A')} success_rate {unmute_success_rate}%, days in state N/A, pass_count {test.get('pass_count')}, fail_count {test.get('fail_count')}, total_runs {unmute_total_runs}, resolution to_unmute\n"
                        unmuted_candidates.add(test_string)
                        unmuted_candidates_debug.append(test_string_debug)
        unmuted_candidates = sorted(unmuted_candidates)
        unmuted_candidates_debug = sorted(unmuted_candidates_debug)
        add_lines_to_file(os.path.join(output_path, 'to_unmute.txt'), unmuted_candidates)
        add_lines_to_file(os.path.join(output_path, 'to_unmute_debug.txt'), unmuted_candidates_debug)
        logging.info(f"Created to_unmute.txt with {len(unmuted_candidates)} candidates")
        logging.info(f"Created to_unmute_debug.txt with {len(unmuted_candidates_debug)} candidates")

        # Кандидаты на удаление из mute (по новым правилам: за 7 дней нет запусков)
        deleted_from_mute = set()
        deleted_from_mute_debug = []
        for test in aggregated_for_mute:
            testsuite = test.get('suite_folder')
            testcase = test.get('test_name')
            test_string = f"{testsuite} {testcase}\n"
            # Проверяем только тесты, которые уже в muted_ya
            if testsuite and testcase and mute_check(testsuite, testcase):
                delete_stats = aggregated_for_delete.get(test.get('full_name'))
                delete_runs = delete_stats.get('pass_count', 0) + delete_stats.get('fail_count', 0) if delete_stats else 0
                if delete_runs == 0:
                    # Вычисляем success_rate для delete
                    delete_total_runs = test.get('pass_count', 0) + test.get('fail_count', 0)
                    delete_success_rate = round((test.get('pass_count', 0) / delete_total_runs * 100) if delete_total_runs > 0 else 0, 1)
                    test_string_debug = f"{testsuite} {testcase} # owner {test.get('owner', 'N/A')} success_rate {delete_success_rate}%, days in state N/A, pass_count {test.get('pass_count')}, fail_count {test.get('fail_count')}, total_runs {delete_total_runs}, resolution to_delete\n"
                    deleted_from_mute.add(test_string)
                    deleted_from_mute_debug.append(test_string_debug)
        deleted_from_mute = sorted(deleted_from_mute)
        deleted_from_mute_debug = sorted(deleted_from_mute_debug)
        add_lines_to_file(os.path.join(output_path, 'to_delete.txt'), deleted_from_mute)
        add_lines_to_file(os.path.join(output_path, 'to_delete_debug.txt'), deleted_from_mute_debug)
        logging.info(f"Created to_delete.txt with {len(deleted_from_mute)} candidates")
        logging.info(f"Created to_delete_debug.txt with {len(deleted_from_mute_debug)} candidates")

        # Дополнительные файлы для анализа
        # 1. muted_ya - deleted
        muted_ya_minus_deleted = set()
        muted_ya_minus_deleted_debug = []
        for test in aggregated_for_mute:
            testsuite = test.get('suite_folder')
            testcase = test.get('test_name')
            if testsuite and testcase and mute_check(testsuite, testcase):
                test_string = f"{testsuite} {testcase}\n"
                # Вычисляем success_rate для muted_ya_minus_deleted
                deleted_total_runs = test.get('pass_count', 0) + test.get('fail_count', 0)
                deleted_success_rate = round((test.get('pass_count', 0) / deleted_total_runs * 100) if deleted_total_runs > 0 else 0, 1)
                test_string_debug = f"{testsuite} {testcase} # owner {test.get('owner', 'N/A')} success_rate {deleted_success_rate}%, days in state N/A\n"
                if test_string not in deleted_tests:
                    muted_ya_minus_deleted.add(test_string)
                    muted_ya_minus_deleted_debug.append(test_string_debug)
        
        muted_ya_minus_deleted = sorted(muted_ya_minus_deleted)
        add_lines_to_file(os.path.join(output_path, 'muted_ya-deleted.txt'), muted_ya_minus_deleted)
        muted_ya_minus_deleted_debug = sorted(muted_ya_minus_deleted_debug)
        add_lines_to_file(os.path.join(output_path, 'muted_ya-deleted_debug.txt'), muted_ya_minus_deleted_debug)
        logging.info(f"Created muted_ya-deleted.txt with {len(muted_ya_minus_deleted)} tests")

        # 2. muted_ya - stable
        muted_ya_minus_stable = set()
        muted_ya_minus_stable_debug = []
        for test in aggregated_for_mute:
            testsuite = test.get('suite_folder')
            testcase = test.get('test_name')
            if testsuite and testcase and mute_check(testsuite, testcase):
                test_string = f"{testsuite} {testcase}\n"
                # Вычисляем success_rate для muted_ya_minus_stable
                stable_total_runs = test.get('pass_count', 0) + test.get('fail_count', 0)
                stable_success_rate = round((test.get('pass_count', 0) / stable_total_runs * 100) if stable_total_runs > 0 else 0, 1)
                test_string_debug = f"{testsuite} {testcase} # owner {test.get('owner', 'N/A')} success_rate {stable_success_rate}%, days in state N/A\n"
                if test_string not in muted_stable_tests:
                    muted_ya_minus_stable.add(test_string)
                    muted_ya_minus_stable_debug.append(test_string_debug)
        
        muted_ya_minus_stable = sorted(muted_ya_minus_stable)
        add_lines_to_file(os.path.join(output_path, 'muted_ya-stable.txt'), muted_ya_minus_stable)
        muted_ya_minus_stable_debug = sorted(muted_ya_minus_stable_debug)
        add_lines_to_file(os.path.join(output_path, 'muted_ya-stable_debug.txt'), muted_ya_minus_stable_debug)
        logging.info(f"Created muted_ya-stable.txt with {len(muted_ya_minus_stable)} tests")

        # 3. muted_ya - stable - deleted
        muted_ya_minus_stable_minus_deleted = set()
        muted_ya_minus_stable_minus_deleted_debug = []
        for test in aggregated_for_mute:
            testsuite = test.get('suite_folder')
            testcase = test.get('test_name')
            if testsuite and testcase and mute_check(testsuite, testcase):
                test_string = f"{testsuite} {testcase}\n"
                # Вычисляем success_rate для muted_ya_minus_stable_minus_deleted
                stable_deleted_total_runs = test.get('pass_count', 0) + test.get('fail_count', 0)
                stable_deleted_success_rate = round((test.get('pass_count', 0) / stable_deleted_total_runs * 100) if stable_deleted_total_runs > 0 else 0, 1)
                test_string_debug = f"{testsuite} {testcase} # owner {test.get('owner', 'N/A')} success_rate {stable_deleted_success_rate}%, days in state N/A\n"
                if test_string not in muted_stable_tests and test_string not in deleted_tests:
                    muted_ya_minus_stable_minus_deleted.add(test_string)
                    muted_ya_minus_stable_minus_deleted_debug.append(test_string_debug)
        
        muted_ya_minus_stable_minus_deleted = sorted(muted_ya_minus_stable_minus_deleted)
        add_lines_to_file(os.path.join(output_path, 'muted_ya-stable-deleted.txt'), muted_ya_minus_stable_minus_deleted)
        muted_ya_minus_stable_minus_deleted_debug = sorted(muted_ya_minus_stable_minus_deleted_debug)
        add_lines_to_file(os.path.join(output_path, 'muted_ya-stable-deleted_debug.txt'), muted_ya_minus_stable_minus_deleted_debug)
        logging.info(f"Created muted_ya-stable-deleted.txt with {len(muted_ya_minus_stable_minus_deleted)} tests")

        # 4. muted_ya - stable - deleted + flaky
        muted_ya_minus_stable_minus_deleted_plus_flaky = set()
        muted_ya_minus_stable_minus_deleted_plus_flaky_debug = []
        
        # Добавляем тесты из muted_ya - stable - deleted
        for test in aggregated_for_mute:
            testsuite = test.get('suite_folder')
            testcase = test.get('test_name')
            if testsuite and testcase and mute_check(testsuite, testcase):
                test_string = f"{testsuite} {testcase}\n"
                # Вычисляем success_rate для muted_ya_minus_stable_minus_deleted_plus_flaky
                flaky_total_runs = test.get('pass_count', 0) + test.get('fail_count', 0)
                flaky_success_rate = round((test.get('pass_count', 0) / flaky_total_runs * 100) if flaky_total_runs > 0 else 0, 1)
                test_string_debug = f"{testsuite} {testcase} # owner {test.get('owner', 'N/A')} success_rate {flaky_success_rate}%, days in state N/A\n"
                if test_string not in muted_stable_tests and test_string not in deleted_tests:
                    muted_ya_minus_stable_minus_deleted_plus_flaky.add(test_string)
                    muted_ya_minus_stable_minus_deleted_plus_flaky_debug.append(test_string_debug)
        
        # Добавляем flaky тесты (если их еще нет)
        for test in aggregated_for_mute:
            testsuite = test.get('suite_folder')
            testcase = test.get('test_name')
            if testsuite and testcase:
                test_string = re.sub(r'\d+/(\d+)\]', r'*/*]', f"{testsuite} {testcase}\n")
                # Вычисляем success_rate для flaky тестов
                flaky_test_total_runs = test.get('pass_count', 0) + test.get('fail_count', 0)
                flaky_test_success_rate = round((test.get('pass_count', 0) / flaky_test_total_runs * 100) if flaky_test_total_runs > 0 else 0, 1)
                test_string_debug = re.sub(r'\d+/(\d+)\]', r'*/*]', f"{testsuite} {testcase}") + f" # owner {test.get('owner', 'N/A')} success_rate {flaky_test_success_rate}%, days in state N/A, pass_count {test.get('pass_count')}, fail count {test.get('fail_count')}\n"
                if (test.get('fail_count', 0) >= 2) or (test.get('fail_count', 0) >= 1 and (test.get('pass_count', 0) + test.get('fail_count', 0)) <= 10):
                    if test_string not in muted_ya_minus_stable_minus_deleted_plus_flaky:
                        muted_ya_minus_stable_minus_deleted_plus_flaky.add(test_string)
                        muted_ya_minus_stable_minus_deleted_plus_flaky_debug.append(test_string_debug)
        
        muted_ya_minus_stable_minus_deleted_plus_flaky = sorted(muted_ya_minus_stable_minus_deleted_plus_flaky)
        add_lines_to_file(os.path.join(output_path, 'muted_ya-stable-deleted+flaky.txt'), muted_ya_minus_stable_minus_deleted_plus_flaky)
        muted_ya_minus_stable_minus_deleted_plus_flaky_debug = sorted(muted_ya_minus_stable_minus_deleted_plus_flaky_debug)
        add_lines_to_file(os.path.join(output_path, 'muted_ya-stable-deleted+flaky_debug.txt'), muted_ya_minus_stable_minus_deleted_plus_flaky_debug)
        logging.info(f"Created muted_ya-stable-deleted+flaky.txt with {len(muted_ya_minus_stable_minus_deleted_plus_flaky)} tests")

        logging.info(f"Muted candidates: {len(muted_candidates)}")
        logging.info(f"Unmuted candidates: {len(unmuted_candidates)}")
        logging.info(f"Deleted from mute: {len(deleted_from_mute)}")
        logging.info(f"Muted_ya - deleted: {len(muted_ya_minus_deleted)}")
        logging.info(f"Muted_ya - stable: {len(muted_ya_minus_stable)}")
        logging.info(f"Muted_ya - stable - deleted: {len(muted_ya_minus_stable_minus_deleted)}")
        logging.info(f"Muted_ya - stable - deleted + flaky: {len(muted_ya_minus_stable_minus_deleted_plus_flaky)}")
    except (KeyError, TypeError) as e:
        logging.error(f"Error processing test data: {e}. Check your query results for valid keys.")
        return []

    return len(muted_candidates)



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
