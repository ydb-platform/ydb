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
    
    query_start_time = datetime.datetime.now()
    logging.info(f"Executing query for branch='{branch}', build_type='{build_type}', days_window={days_window}")
    logging.info(f"Date range: {start_date} to {end_date}")
    
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
        days_in_state
    FROM `test_results/analytics/tests_monitor`
    WHERE date_window = CurrentUtcDate() 
        AND branch = '{branch}' 
        AND build_type = '{build_type}'
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

def aggregate_test_data(all_data, period_days):
    """
    Универсальная функция для агрегации тестовых данных за указанный период
    """
    today = datetime.date.today()
    base_date = datetime.date(1970, 1, 1)
    start_date = today - datetime.timedelta(days=period_days-1)
    start_days = (start_date - base_date).days
    
    # Агрегируем данные по full_name
    aggregated = {}
    for test in all_data:
        if test.get('date_window', 0) >= start_days:
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
                    'state': test.get('state'),
                    'days_in_state': test.get('days_in_state'),
                    'date_window': test.get('date_window')
                }
            
            # Суммируем статистику
            aggregated[full_name]['pass_count'] += test.get('pass_count', 0)
            aggregated[full_name]['fail_count'] += test.get('fail_count', 0)
            aggregated[full_name]['mute_count'] += test.get('mute_count', 0)
            aggregated[full_name]['skip_count'] += test.get('skip_count', 0)
    
    # Вычисляем success_rate и создаем summary для каждого теста
    for test_data in aggregated.values():
        total_runs = test_data['pass_count'] + test_data['fail_count'] + test_data['mute_count'] + test_data['skip_count']
        if total_runs > 0:
            test_data['success_rate'] = round((test_data['pass_count'] / total_runs) * 100, 1)
        else:
            test_data['success_rate'] = 0.0
        
        # Создаем summary для тикета
        test_data['summary'] = f"p-{test_data['pass_count']}, f-{test_data['fail_count']}, m-{test_data['mute_count']}, s-{test_data['skip_count']}, total-{total_runs}"
    
    return list(aggregated.values())


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

def create_debug_string(test, resolution, success_rate=None, period_days=None):
    """Создает debug строку для теста"""
    if success_rate is None:
        success_rate = calculate_success_rate(test)
    
    testsuite = test.get('suite_folder')
    testcase = test.get('test_name')
    runs = test.get('pass_count', 0) + test.get('fail_count', 0)
    
    debug_string = f"{testsuite} {testcase} # owner {test.get('owner', 'N/A')} success_rate {success_rate}%"
    
    if period_days:
        debug_string += f" (last {period_days} days)"
    
    debug_string += f", p-{test.get('pass_count')}, f-{test.get('fail_count')},m-{test.get('mute_count')}, s-{test.get('skip_count')}, runs-{runs}, resolution {resolution}"
    
    return debug_string + "\n"

def is_flaky_test(test, all_data, period_days=3):
    """Проверяет, является ли тест flaky за указанный период"""
    # Агрегируем данные за период
    aggregated_data = aggregate_test_data(all_data, period_days)
    
    # Ищем наш тест в агрегированных данных
    test_data = None
    for agg_test in aggregated_data:
        if agg_test['full_name'] == test.get('full_name'):
            test_data = agg_test
            break
    
    if not test_data:
        return False
    
    # Обновляем данные теста для debug
    test['pass_count'] = test_data['pass_count']
    test['fail_count'] = test_data['fail_count']
    test['period_days'] = period_days
    
    total_runs = test_data['pass_count'] + test_data['fail_count']
    return (test_data['fail_count'] >= 2) or (test_data['fail_count'] >= 1 and total_runs <= 10)

def is_unmute_candidate(test, all_data, period_days=2):
    """Проверяет, является ли тест кандидатом на размьют за указанный период"""
    # Агрегируем данные за период
    aggregated_data = aggregate_test_data(all_data, period_days)
    
    # Ищем наш тест в агрегированных данных
    test_data = None
    for agg_test in aggregated_data:
        if agg_test['full_name'] == test.get('full_name'):
            test_data = agg_test
            break
    
    if not test_data:
        return False
    
    # Обновляем данные теста для debug
    test['pass_count'] = test_data['pass_count']
    test['fail_count'] = test_data['fail_count']
    test['mute_count'] = test_data['mute_count']
    test['period_days'] = period_days
    
    total_runs = test_data['pass_count'] + test_data['fail_count'] + test_data['mute_count']
    total_fails = test_data['fail_count'] + test_data['mute_count']
    
    return total_runs > 4 and total_fails == 0

def is_delete_candidate(test, all_data, period_days=7):
    """Проверяет, является ли тест кандидатом на удаление из mute за указанный период"""
    # Агрегируем данные за период
    aggregated_data = aggregate_test_data(all_data, period_days)
    
    # Ищем наш тест в агрегированных данных
    test_data = None
    for agg_test in aggregated_data:
        if agg_test['full_name'] == test.get('full_name'):
            test_data = agg_test
            break
    
    if not test_data:
        return False
    
    # Обновляем данные теста для debug
    test['pass_count'] = test_data['pass_count']
    test['fail_count'] = test_data['fail_count']
    test['mute_count'] = test_data['mute_count']
    test['skip_count'] = test_data['skip_count']
    test['period_days'] = period_days
    
    total_runs = test_data['pass_count'] + test_data['fail_count'] + test_data['mute_count'] + test_data['skip_count']
    
    return total_runs == 0

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
                period_days = test.get('period_days')
                debug_string = create_debug_string(test, resolution, period_days=period_days)
                debug_list.append(debug_string)
    
    return sorted(result_set), sorted(debug_list)

def write_file_set(file_path, test_set, debug_list=None):
    """Записывает набор тестов в файл"""
    add_lines_to_file(file_path, test_set)
    if debug_list:
        debug_path = file_path.replace('.txt', '_debug.txt')
        add_lines_to_file(debug_path, debug_list)
    logging.info(f"Created {os.path.basename(file_path)} with {len(test_set)} tests")

def apply_and_add_mutes(all_data, output_path, mute_check):
    output_path = os.path.join(output_path, 'mute_update')
    logging.info(f"Creating mute files in directory: {output_path}")
    
    # Получаем уникальные тесты для обработки (используем максимальный период для получения всех тестов)
    unique_tests = {}
    for test in all_data:
        full_name = test.get('full_name')
        if full_name not in unique_tests:
            unique_tests[full_name] = test
    
    aggregated_for_mute = list(unique_tests.values())
    aggregated_for_mute = sorted(aggregated_for_mute, key=lambda d: d['full_name'])
    
    logging.info(f"Processing {len(aggregated_for_mute)} unique tests for mute analysis")

    try:
        # 1. Кандидаты на mute
        def is_mute_candidate(test):
            return is_flaky_test(test, all_data, period_days=3)
        
        to_mute, to_mute_debug = create_file_set(
            aggregated_for_mute, is_mute_candidate, use_wildcards=True, resolution='to_mute'
        )
        write_file_set(os.path.join(output_path, 'to_mute.txt'), to_mute, to_mute_debug)
        
        # 2. Кандидаты на размьют
        def is_unmute_candidate_wrapper(test):
            return is_unmute_candidate(test, all_data, period_days=2)
        
        to_unmute, to_unmute_debug = create_file_set(
            aggregated_for_mute, is_unmute_candidate_wrapper, mute_check, resolution='to_unmute'
        )
        
        # Для wildcard-паттернов: проверяем, что ВСЕ chunk'и подходят под условия размьюта
        wildcard_unmute_candidates = []
        wildcard_unmute_debug = []
        
        # Группируем тесты по wildcard-паттернам
        wildcard_groups = {}
        for test in aggregated_for_mute:
            if mute_check and mute_check(test.get('suite_folder'), test.get('test_name')):
                wildcard_pattern = create_test_string(test, use_wildcards=True)
                if wildcard_pattern not in wildcard_groups:
                    wildcard_groups[wildcard_pattern] = []
                wildcard_groups[wildcard_pattern].append(test)
        
        # Проверяем каждый wildcard-паттерн
        for wildcard_pattern, chunks in wildcard_groups.items():
            # Если ВСЕ chunk'и подходят под условия размьюта
            all_chunks_unmutable = all(
                is_unmute_candidate(chunk, all_data, period_days=2)
                for chunk in chunks
            )
            
            if all_chunks_unmutable:
                wildcard_unmute_candidates.append(wildcard_pattern)
                # Берем debug-строку первого chunk'а
                if chunks:
                    debug_string = create_debug_string(chunks[0], 'to_unmute', period_days=2)
                    wildcard_unmute_debug.append(debug_string)
        
        # Объединяем обычные и wildcard кандидаты
        to_unmute = sorted(list(set(to_unmute) | set(wildcard_unmute_candidates)))
        to_unmute_debug = sorted(list(set(to_unmute_debug) | set(wildcard_unmute_debug)))
        
        write_file_set(os.path.join(output_path, 'to_unmute.txt'), to_unmute, to_unmute_debug)
        
        # 3. Кандидаты на удаление из mute (to_delete)
        def is_delete_candidate_wrapper(test):
            return is_delete_candidate(test, all_data, period_days=7)
        
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
        
        # 10. muted_ya_changes - файл с изменениями
        muted_ya_changes = []
        muted_ya_changes_debug = []
        
        # Тесты для добавления (+++)
        for test in to_mute:
            muted_ya_changes.append(f"+++ {test}")
            # Находим соответствующую debug-строку
            if test in to_mute_debug_dict:
                muted_ya_changes_debug.append(f"+++ {to_mute_debug_dict[test]}")
        
        # Тесты для размьюта (---)
        for test in to_unmute:
            muted_ya_changes.append(f"--- {test}")
            # Находим соответствующую debug-строку из all_muted_ya_debug_dict
            if test in all_muted_ya_debug_dict:
                muted_ya_changes_debug.append(f"--- {all_muted_ya_debug_dict[test]}")
        
        # Тесты для удаления (xxx)
        for test in to_delete:
            muted_ya_changes.append(f"xxx {test}")
            # Находим соответствующую debug-строку из all_muted_ya_debug_dict
            if test in all_muted_ya_debug_dict:
                muted_ya_changes_debug.append(f"xxx {all_muted_ya_debug_dict[test]}")
        
        # Тесты без изменений (без префикса)
        unchanged_tests = [t for t in all_muted_ya if t not in to_unmute_set and t not in to_delete_set]
        for test in unchanged_tests:
            muted_ya_changes.append(test)
            if test in all_muted_ya_debug_dict:
                muted_ya_changes_debug.append(all_muted_ya_debug_dict[test])
        
        # Сортируем по алфавиту
        muted_ya_changes = sorted(muted_ya_changes)
        muted_ya_changes_debug = sorted(muted_ya_changes_debug)
        
        write_file_set(os.path.join(output_path, 'muted_ya_changes.txt'), muted_ya_changes, muted_ya_changes_debug)
        
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
                            'date_window': (base_date + datetime.timedelta(days=test.get('date_window'))).date(),
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

    logging.info("Executing single query for 7 days window...")
    
    # Один запрос за максимальный период (7 дней)
    all_data = execute_query(args.branch, days_window=7)
    logging.info(f"Query returned {len(all_data)} test records")
    
    # Используем универсальную агрегацию для разных периодов
    aggregated_for_mute = aggregate_test_data(all_data, 3)  # 3 дня для mute
    aggregated_for_unmute = aggregate_test_data(all_data, 2)  # 2 дня для unmute
    aggregated_for_delete = aggregate_test_data(all_data, 7)  # 7 дней для delete
    
    logging.info(f"Aggregated data: mute={len(aggregated_for_mute)}, unmute={len(aggregated_for_unmute)}, delete={len(aggregated_for_delete)}")
    
    if args.mode == 'update_muted_ya':
        output_path = args.output_folder
        os.makedirs(output_path, exist_ok=True)
        logging.info(f"Creating mute files in: {output_path}")
        apply_and_add_mutes(all_data, output_path, mute_check)

    elif args.mode == 'create_issues':
        file_path = args.file_path
        logging.info(f"Creating issues from file: {file_path}")
        
        # Используем универсальную агрегацию для create_mute_issues (за 3 дня, как для mute)
        aggregated_tests_list = aggregate_test_data(all_data, 3)
        create_mute_issues(aggregated_tests_list, file_path, close_issues=args.close_issues)
    
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
