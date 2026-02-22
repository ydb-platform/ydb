#!/usr/bin/env python3
import argparse
import datetime
import os
import re
import ydb
import logging
import sys
from pathlib import Path
from collections import defaultdict

# Add the parent directory to the path to import update_mute_issues
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from mute_check import YaMuteCheck
from pattern_rules_loader import (
    load_rules,
    get_mute_rule,
    get_unmute_rule,
    get_delete_rule,
    get_quarantine_graduation_rule,
    get_rule_params,
)
from update_mute_issues import (
    create_and_add_issue_to_project,
    generate_github_issue_title_and_body,
    get_muted_tests_from_issues,
    close_unmuted_issues,
)

# Add analytics directory to path for ydb_wrapper import
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'analytics'))
from ydb_wrapper import YDBWrapper
from mute_decisions import write_mute_decisions

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

dir = os.path.dirname(__file__)
repo_path = f"{dir}/../../../"
muted_ya_path = '.github/config/muted_ya.txt'
quarantine_path = '.github/config/quarantine.txt'

# Дефолты (если правила не загружены)
DEFAULT_MUTE_DAYS = 4
DEFAULT_UNMUTE_DAYS = 7
DEFAULT_DELETE_DAYS = 7

def is_chunk_test(test):
    # Сначала смотрим на поле is_test_chunk, если оно есть
    if 'is_test_chunk' in test:
        return bool(test['is_test_chunk'])
    # Фоллбек на старую логику по имени
    return "chunk" in test.get('test_name', '').lower()

def get_wildcard_unmute_candidates(aggregated_for_unmute, mute_check, is_unmute_candidate):
    """
    Возвращает список (pattern, debug_string) для wildcard-паттернов, где все chunk'и проходят фильтр на размьют.
    Если хотя бы один chunk не проходит фильтр, паттерн не попадает в размьют.
    Debug-строка берётся от любого chunk'а, прошедшего фильтр (обычно первого).
    Это гарантирует, что если размьючивается паттерн, то размьючиваются все chunk'и, и наоборот.
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
        passed_chunks = [chunk for chunk in chunks if is_unmute_candidate(chunk, aggregated_for_unmute)]
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
    Возвращает список (pattern, debug_string) для wildcard-паттернов, где все chunk'и проходят фильтр на delete (удаление mute).
    Если хотя бы один chunk не проходит фильтр, паттерн не попадает в delete.
    Debug-строка берётся от любого chunk'а, прошедшего фильтр (обычно первого).
    Это гарантирует, что если удаляется mute по паттерну, то удаляются все chunk'и, и наоборот.
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
        passed_chunks = [chunk for chunk in chunks if is_delete_candidate(chunk, aggregated_for_delete)]
        if len(passed_chunks) == len(chunks) and chunks:
            debug = create_debug_string(
                passed_chunks[0],
                period_days=passed_chunks[0].get('period_days'),
                date_window=passed_chunks[0].get('date_window')
            )
            result.append((pattern, debug))
    return result


def execute_query(branch='main', build_type='relwithdebinfo', days_window=1):
    # Получаем today
    today = datetime.date.today()
    start_date = today - datetime.timedelta(days=days_window-1)
    end_date = today
    
    logging.info(f"Executing query for branch='{branch}', build_type='{build_type}', days_window={days_window}")
    logging.info(f"Date range: {start_date} to {end_date}")
    
    try:
        with YDBWrapper() as ydb_wrapper:
            # Check credentials
            if not ydb_wrapper.check_credentials():
                return []
            
            # Get table path from config
            tests_monitor_table = ydb_wrapper.get_table_path("tests_monitor")
            
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
    WHERE date_window >= CurrentUtcDate() - 7*Interval("P1D")
        AND branch = '{branch}' 
        AND build_type = '{build_type}'
    '''
            
            logging.info(f"SQL Query:\n{query_string}")
            
            logging.info("Successfully connected to YDB")
            
            logging.info("Starting to fetch results...")
            results = ydb_wrapper.execute_scan_query(query_string, query_name=f"get_tests_monitor_data_{branch}")
            
            # Filter out suite-level entries (aggregates, not individual tests)
            # Similar to generate-summary.py which skips results with suite=True
            # Suite tests have test_name like "unittest", "py3test", "gtest"
            suite_test_names = {'unittest', 'py3test', 'gtest'}
            filtered_results = [
                result for result in results
                if result.get('test_name') and result.get('test_name') not in suite_test_names
            ]
            
            logging.info(f"Query completed successfully. Total rows returned: {len(results)}, after filtering suite tests: {len(filtered_results)}")
            return filtered_results
        
    except Exception as e:
        logging.error(f"Error executing query: {e}")
        logging.error(f"Query parameters: branch='{branch}', build_type='{build_type}', days_window={days_window}")
        logging.error(f"Date range: {start_date} to {end_date}")
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
    
    # Helper function to convert date to days if needed
    def to_days(date_value):
        if date_value is None:
            return -1
        if isinstance(date_value, datetime.date):
            return (date_value - base_date).days
        return date_value
    
    logging.info(f"Starting aggregation for {period_days} days period...")
    logging.info(f"Processing {len(all_data)} test records...")
    
    # Агрегируем данные по full_name
    aggregated = {}
    processed_count = 0
    total_count = len(all_data)
    
    # Сортируем записи по дате, чтобы история состояний формировалась в хронологическом порядке
    sorted_data = sorted(
        all_data,
        key=lambda test: (
            to_days(test.get('date_window')),
            test.get('full_name') or ''
        )
    )
    
    for test in sorted_data:
        processed_count += 1
        # Показываем прогресс каждые 1000 записей или каждые 10%
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
                    'is_muted': test.get('is_muted'),
                    'is_muted_date': test.get('date_window'),  # Сохраняем дату для is_muted
                    'state': test.get('state'),
                    'state_history': [test.get('state')],  # Начинаем историю состояний
                    'state_dates': [test.get('date_window')],  # История дат состояний
                    'days_in_state': test.get('days_in_state'),
                    'date_window': test.get('date_window'),
                    'period_days': period_days,  # <--- добавляем сюда
                    'is_test_chunk': test.get('is_test_chunk', 0),
                }
            else:
                # Добавляем состояние в историю, если оно отличается от последнего
                current_state = test.get('state')
                current_date = test.get('date_window')
                if current_state and (not aggregated[full_name]['state_history'] or aggregated[full_name]['state_history'][-1] != current_state):
                    aggregated[full_name]['state_history'].append(current_state)
                    aggregated[full_name]['state_dates'].append(current_date)
                
                # Обновляем is_muted если текущая дата новее
                if to_days(test.get('date_window', 0)) > to_days(aggregated[full_name].get('is_muted_date', 0)):
                    aggregated[full_name]['is_muted'] = test.get('is_muted')
                    aggregated[full_name]['is_muted_date'] = test.get('date_window')
            
            # Суммируем статистику
            aggregated[full_name]['pass_count'] += test.get('pass_count', 0)
            aggregated[full_name]['fail_count'] += test.get('fail_count', 0)
            aggregated[full_name]['mute_count'] += test.get('mute_count', 0)
            aggregated[full_name]['skip_count'] += test.get('skip_count', 0)
    
    # Вычисляем success_rate, создаем summary и конкатенируем state для каждого теста
    date_window_range = f"{start_date.strftime('%Y-%m-%d')}:{today.strftime('%Y-%m-%d')}"
    for test_data in aggregated.values():
        test_data['date_window'] = date_window_range
        total_runs = test_data['pass_count'] + test_data['fail_count'] + test_data['mute_count'] + test_data['skip_count']
        if total_runs > 0:
            test_data['success_rate'] = round((test_data['pass_count'] / total_runs) * 100, 1)
        else:
            test_data['success_rate'] = 0.0
        
        # Создаем summary для тикета (без дублирования state, так как он уже есть в начале строки)
        test_data['summary'] = f"p-{test_data['pass_count']}, f-{test_data['fail_count']}, m-{test_data['mute_count']}, s-{test_data['skip_count']}, total-{total_runs}"
        
        # Конкатенируем state по дням с датами
        if 'state_history' in test_data and len(test_data['state_history']) > 1:
            # Создаем строку с состояниями и датами
            state_with_dates = []
            for i, (state, date) in enumerate(zip(test_data['state_history'], test_data['state_dates'])):
                if date:
                    # Преобразуем дату в читаемый формат
                    if isinstance(date, int):
                        date_obj = base_date + datetime.timedelta(days=date)
                    else:
                        date_obj = date
                    date_str = date_obj.strftime('%Y-%m-%d')
                    state_with_dates.append(f"{state}({date_str})")
                else:
                    state_with_dates.append(state)
            test_data['state'] = '->'.join(state_with_dates)
        # Если только одно состояние, оставляем как есть
    
    logging.info(f"Aggregation completed: {len(aggregated)} unique tests aggregated from {total_count} records")
    return list(aggregated.values())


def calculate_success_rate(test):
    """Вычисляет success rate для теста"""
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
    """Функция для сортировки строк, игнорирующая префиксы +++, ---, xxx"""
    # Убираем префиксы +++, ---, xxx для сортировки
    if line.startswith('+++ ') or line.startswith('--- ') or line.startswith('xxx '):
        return line[4:]  # Убираем первые 4 символа (префикс + пробел)
    return line  # Для строк без префикса возвращаем как есть


def create_debug_string(test, success_rate=None, period_days=None, date_window=None):
    """Создает debug строку для теста"""
    if success_rate is None:
        success_rate = calculate_success_rate(test)
    
    testsuite = test.get('suite_folder')
    testcase = test.get('test_name')
    runs = test.get('pass_count', 0) + test.get('fail_count', 0) + test.get('mute_count', 0) 
    
    debug_string = f"{testsuite} {testcase} # owner {test.get('owner', 'N/A')} success_rate {success_rate}%"
    # Показываем оба периода: days и диапазон дат, если есть
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

def is_mute_candidate(test, aggregated_data, params=None):
    """Проверяет, является ли тест кандидатом на mute за указанный период.
    params: min_failures_high, min_failures_low, min_runs_threshold (from rule)
    """
    p = params or {}
    min_fail_high = p.get('min_failures_high', 3)
    min_fail_low = p.get('min_failures_low', 2)
    min_runs_threshold = p.get('min_runs_threshold', 10)

    test_data = None
    for agg_test in aggregated_data:
        if agg_test['full_name'] == test.get('full_name'):
            test_data = agg_test
            break

    if not test_data:
        return False

    test['pass_count'] = test_data['pass_count']
    test['fail_count'] = test_data['fail_count']
    test['period_days'] = test_data.get('period_days')
    test['is_muted'] = test_data.get('is_muted', False)

    if test_data.get('is_muted', False):
        return False

    total_runs = test_data['pass_count'] + test_data['fail_count']
    result = (
        (test_data['fail_count'] >= min_fail_high and total_runs > min_runs_threshold)
        or (test_data['fail_count'] >= min_fail_low and total_runs <= min_runs_threshold)
    )

    if not test_data.get('is_muted', False):
        logging.debug(f"MUTE_CHECK: {test.get('full_name')} - runs:{total_runs}, fails:{test_data['fail_count']}, state:{test_data.get('state')}, muted:{test_data.get('is_muted')}, result:{result}")

    return result

def is_unmute_candidate(test, aggregated_data, params=None):
    """Проверяет, является ли тест кандидатом на размьют за указанный период.
    params: min_runs, max_fails (from rule)
    """
    p = params or {}
    min_runs = p.get('min_runs', 4)
    max_fails = p.get('max_fails', 0)

    test_data = None
    for agg_test in aggregated_data:
        if agg_test['full_name'] == test.get('full_name'):
            test_data = agg_test
            break

    if not test_data:
        return False

    test['pass_count'] = test_data['pass_count']
    test['fail_count'] = test_data['fail_count']
    test['mute_count'] = test_data['mute_count']
    test['period_days'] = test_data.get('period_days')
    test['is_muted'] = test_data.get('is_muted', False)

    total_runs = test_data['pass_count'] + test_data['fail_count'] + test_data['mute_count']
    total_fails = test_data['fail_count'] + test_data['mute_count']

    result = total_runs >= min_runs and total_fails <= max_fails

    if test_data.get('is_muted', False):
        logging.debug(f"UNMUTE_CHECK: {test.get('full_name')} - runs:{total_runs}, fails:{total_fails}, mute_count:{test_data['mute_count']}, state:{test_data.get('state')}, muted:{test_data.get('is_muted')}, result:{result}")

    return result

def is_delete_candidate(test, aggregated_data, params=None):
    """Проверяет, является ли тест кандидатом на удаление из mute за указанный период"""
    test_data = None
    for agg_test in aggregated_data:
        if agg_test['full_name'] == test.get('full_name'):
            test_data = agg_test
            break

    if not test_data:
        return False

    test['pass_count'] = test_data['pass_count']
    test['fail_count'] = test_data['fail_count']
    test['mute_count'] = test_data['mute_count']
    test['skip_count'] = test_data['skip_count']
    test['period_days'] = test_data.get('period_days')
    test['is_muted'] = test_data.get('is_muted', False)

    total_runs = test_data['pass_count'] + test_data['fail_count'] + test_data['mute_count'] + test_data['skip_count']
    result = total_runs == 0

    if test_data.get('is_muted', False):
        logging.debug(f"DELETE_CHECK: {test.get('full_name')} - runs:{total_runs}, muted:{test_data.get('is_muted')}, result:{result}")

    return result

def create_file_set(aggregated_for_mute, filter_func, mute_check=None, use_wildcards=False, resolution=None, exclude_check=None):
    """Создает набор тестов для файла на основе фильтра"""
    result_set = set()
    debug_list = []
    total = len(aggregated_for_mute)
    last_percent = -1
    for idx, test in enumerate(aggregated_for_mute, 1):
        testsuite = test.get('suite_folder')
        testcase = test.get('test_name')
        
        if not testsuite or not testcase:
            continue
        
        # Исключаем тесты (напр. quarantine)
        if exclude_check and exclude_check(testsuite, testcase):
            continue
        # Проверяем mute_check если передан
        if mute_check and not mute_check(testsuite, testcase):
            continue
        # Прогресс-бар
        percent = int(idx / total * 100)
        if percent != last_percent and (percent % 5 == 0 or percent == 100):
            print(f"\r[create_file_set] Progress: {percent}% ({idx}/{total})", end="")
            last_percent = percent
        # Применяем фильтр
        if filter_func(test):
            test_string = create_test_string(test, use_wildcards)
            result_set.add(test_string)
            
            if resolution:
                debug_string = create_debug_string(
                    test,
                    period_days=test.get('period_days'),
                    date_window=test.get('date_window')
                )
                debug_list.append(debug_string)
    
    # Принудительно показываем 100% если не показали
    if last_percent != 100:
        print(f"\r[create_file_set] Progress: 100% ({total}/{total})", end="")
    print()  # Перевод строки после прогресса
    return sorted(list(result_set)), sorted(debug_list)

def write_file_set(file_path, test_set, debug_list=None, sort_without_prefixes=False):
    # По умолчанию игнорируем префиксы для правильной сортировки
    if not sort_without_prefixes:
        sorted_test_set = sorted(test_set)
    else:
        sorted_test_set = sorted(test_set, key=sort_key_without_prefix)
    
    # Сортируем debug_list если он есть
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

def _parse_mute_file(path):
    """Parse mute file, return set of non-empty, non-comment lines."""
    if not path or not os.path.exists(path):
        return set()
    with open(path) as f:
        return set(
            l.strip()
            for l in f
            if l.strip() and not l.strip().startswith("#")
        )


def get_quarantine_graduation(quarantine_tests, aggregated_1day, params=None):
    """
    Rule: min_runs in window AND min_passes → graduate (remove from quarantine).
    params: min_runs, min_passes (from rule)
    """
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
            logging.info(f"Quarantine graduation: {test_line} (runs={total_runs}, pass={pass_count})")
    return graduated


def apply_and_add_mutes(
    all_data,
    output_path,
    mute_check,
    aggregated_for_mute,
    aggregated_for_unmute,
    aggregated_for_delete,
    quarantine_check=None,
    quarantine_path=None,
    to_graduated=None,
    mute_rule_params=None,
    unmute_rule_params=None,
    delete_rule_params=None,
    output_file=None,
):
    output_path = os.path.join(output_path, 'mute_update')
    to_graduated = to_graduated or set()
    mute_rule_params = mute_rule_params or {}
    unmute_rule_params = unmute_rule_params or {}
    delete_rule_params = delete_rule_params or {}
    logging.info(f"Creating mute files in directory: {output_path}")
    
    # Получаем уникальные тесты для обработки (используем максимальный период для получения всех тестов)
    
    # Добавляем статистику по замьюченным тестам
    muted_tests = [test for test in aggregated_for_mute if test.get('is_muted', False)]
    logging.info(f"Total muted tests found: {len(muted_tests)}")


    try:
        # 1. Кандидаты на mute (исключаем quarantine — защита от re-mute)
        def is_mute_candidate_wrapper(test):
            return is_mute_candidate(test, aggregated_for_mute, mute_rule_params)
        
        to_mute, to_mute_debug = create_file_set(
            aggregated_for_mute, is_mute_candidate_wrapper, use_wildcards=True, resolution='to_mute',
            exclude_check=quarantine_check
        )
        write_file_set(os.path.join(output_path, 'to_mute.txt'), to_mute, to_mute_debug)
        
        # 2. Кандидаты на размьют
        def is_unmute_candidate_wrapper(test):
            if is_chunk_test(test):
                return False  # Не размьючивать chunk поштучно
            return is_unmute_candidate(test, aggregated_for_unmute, unmute_rule_params)


        def _is_unmute_for_wildcard(test, agg):
            return is_unmute_candidate(test, agg, unmute_rule_params)

        # Обычные тесты на размьют (по chunk'ам)
        to_unmute, to_unmute_debug = create_file_set(
            aggregated_for_unmute, is_unmute_candidate_wrapper, mute_check, resolution='to_unmute'
        )
        
        # Wildcard-паттерны на размьют:
        wildcard_unmute = get_wildcard_unmute_candidates(aggregated_for_unmute, mute_check, _is_unmute_for_wildcard)
        wildcard_unmute_patterns = [p for p, d in wildcard_unmute]
        wildcard_unmute_debugs = [d for p, d in wildcard_unmute]
        
        # Объединяем поштучные и wildcard-результаты
        to_unmute = sorted(list(set(to_unmute) | set(wildcard_unmute_patterns)))
        to_unmute_debug = sorted(list(set(to_unmute_debug) | set(wildcard_unmute_debugs)))
        
        write_file_set(os.path.join(output_path, 'to_unmute.txt'), to_unmute, to_unmute_debug)
        
        # 3. Кандидаты на удаление из mute (to_delete)
        def is_delete_candidate_wrapper(test):
            if is_chunk_test(test):
                return False  # Не удалять chunk поштучно
            return is_delete_candidate(test, aggregated_for_delete, delete_rule_params)


        def _is_delete_for_wildcard(test, agg):
            return is_delete_candidate(test, agg, delete_rule_params)

        # Обычные тесты на delete (по chunk'ам)
        to_delete, to_delete_debug = create_file_set(
            aggregated_for_delete, is_delete_candidate_wrapper, mute_check, resolution='to_delete'
        )
        
        # Wildcard-паттерны на delete:
        wildcard_delete = get_wildcard_delete_candidates(aggregated_for_delete, mute_check, _is_delete_for_wildcard)
        wildcard_delete_patterns = [p for p, d in wildcard_delete]
        wildcard_delete_debugs = [d for p, d in wildcard_delete]
        
        # Объединяем поштучные и wildcard-результаты
        to_delete = sorted(list(set(to_delete) | set(wildcard_delete_patterns)))
        to_delete_debug = sorted(list(set(to_delete_debug) | set(wildcard_delete_debugs)))
        
        write_file_set(os.path.join(output_path, 'to_delete.txt'), to_delete, to_delete_debug)
        
        # 4. muted_ya (все замьюченные сейчас)
        all_muted_ya, all_muted_ya_debug = create_file_set(
            all_data, lambda test: mute_check(test.get('suite_folder'), test.get('test_name')) if mute_check else True, use_wildcards=True, resolution='muted_ya'
        )
        write_file_set(os.path.join(output_path, 'muted_ya.txt'), all_muted_ya, all_muted_ya_debug)
        to_mute_set = set(to_mute)
        to_unmute_set = set(to_unmute)
        to_delete_set = set(to_delete)
        all_muted_ya_set = set(all_muted_ya)
        
         # Создаем словари для быстрого поиска debug-строк

        # Универсальный словарь: ключ — строка теста (с wildcard или без), значение — debug-строка
        test_debug_dict = {}
        wildcard_to_chunks = defaultdict(list)
        for test in aggregated_for_mute + aggregated_for_unmute + aggregated_for_delete:
            test_str = create_test_string(test, use_wildcards=False)  # без .strip()
            debug_str = create_debug_string(test)
            test_debug_dict[test_str] = debug_str
            if is_chunk_test(test):
                wildcard_key = create_test_string(test, use_wildcards=True)
                wildcard_to_chunks[wildcard_key].append(test)
        # Формируем debug-строку для wildcard
        for wildcard, chunks in wildcard_to_chunks.items():
            N = len(chunks)
            m = sum(1 for t in chunks if t.get('is_muted'))
            
            # Собираем статистику по всем chunks для объяснения решения
            total_pass = sum(t.get('pass_count', 0) for t in chunks)
            total_fail = sum(t.get('fail_count', 0) for t in chunks)
            total_mute = sum(t.get('mute_count', 0) for t in chunks)
            total_skip = sum(t.get('skip_count', 0) for t in chunks)
            total_runs = total_pass + total_fail + total_mute + total_skip
            
            # Определяем причину попадания в список
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

        # 8. muted_ya-to-delete-to-unmute-to_graduated (graduated = quarantine passed, remove from both)
        muted_ya_minus_to_delete_to_unmute = [t for t in all_muted_ya if t not in to_delete and t not in to_unmute and t not in to_graduated]
        muted_ya_minus_to_delete_to_unmute_debug = []
        for test in muted_ya_minus_to_delete_to_unmute:
            debug_val = test_debug_dict.get(test, "NO DEBUG INFO")
            muted_ya_minus_to_delete_to_unmute_debug.append(debug_val)
        write_file_set(os.path.join(output_path, 'muted_ya-to-delete-to-unmute.txt'), muted_ya_minus_to_delete_to_unmute, muted_ya_minus_to_delete_to_unmute_debug)

        # 9. muted_ya-to-delete-to-unmute+to_mute
        # Вместо set используем list для полного соответствия
        muted_ya_minus_to_delete_to_unmute_plus_to_mute = list(muted_ya_minus_to_delete_to_unmute) + [t for t in to_mute if t not in muted_ya_minus_to_delete_to_unmute]
        muted_ya_minus_to_delete_to_unmute_plus_to_mute_debug = []
        for test in muted_ya_minus_to_delete_to_unmute_plus_to_mute:
            debug_val = test_debug_dict.get(test, "NO DEBUG INFO")
            muted_ya_minus_to_delete_to_unmute_plus_to_mute_debug.append(debug_val)
        write_file_set(os.path.join(output_path, 'muted_ya-to-delete-to-unmute+to_mute.txt'), muted_ya_minus_to_delete_to_unmute_plus_to_mute, muted_ya_minus_to_delete_to_unmute_plus_to_mute_debug)
        # Final output: new_muted_ya.txt or custom output_file (for per-build-type)
        final_output = output_file or 'new_muted_ya.txt'
        write_file_set(os.path.join(output_path, final_output), muted_ya_minus_to_delete_to_unmute_plus_to_mute, muted_ya_minus_to_delete_to_unmute_plus_to_mute_debug)
        
        # 10. muted_ya_changes - файл с изменениями (новая логика)
        all_test_strings = sorted(all_muted_ya_set | to_mute_set | to_unmute_set | to_delete_set, key=sort_key_without_prefix)
        # Гарантируем 1:1 соответствие между .txt и debug.txt
        muted_ya_changes = []
        muted_ya_changes_debug = []
        for test_str in all_test_strings:  # all_test_strings должен быть list, не set
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
        return (
            to_mute, to_unmute, to_delete,
            to_mute_debug, to_unmute_debug, to_delete_debug,
        )

    except (KeyError, TypeError) as e:
        logging.error(f"Error processing test data: {e}. Check your query results for valid keys.")
        return ([], [], [], [], [], [])

    return ([], [], [], [], [], [])



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
                            'date_window': test.get('date_window', 'N/A'),
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
    with YDBWrapper() as ydb_wrapper:
        # Check credentials
        if not ydb_wrapper.check_credentials():
            return 1

        logging.info(f"Starting mute worker with mode: {args.mode}")
        logging.info(f"Branch: {args.branch}")
        
        # Используем переданный файл или дефолтный
        input_muted_ya_path = getattr(args, 'muted_ya_file', muted_ya_path)
        logging.info(f"Using muted_ya file: {input_muted_ya_path}")
        
        mute_check = YaMuteCheck()
        mute_check.load(input_muted_ya_path)
        logging.info(f"Loaded muted_ya.txt with {len(mute_check.regexps)} test patterns")

        quarantine_check = None
        input_quarantine_path = getattr(args, 'quarantine_file', quarantine_path)
        if os.path.exists(input_quarantine_path):
            quarantine_check = YaMuteCheck()
            quarantine_check.load(input_quarantine_path)
            logging.info(f"Loaded quarantine.txt with {len(quarantine_check.regexps)} tests (protected from re-mute)")
        else:
            logging.info(f"Quarantine file not found: {input_quarantine_path}, skipping")

        # Загружаем правила
        build_type = getattr(args, 'build_type', 'relwithdebinfo')
        rules_path = getattr(args, 'rules_file', None)
        rules = load_rules(rules_path)
        mute_rule = get_mute_rule(rules, build_type)
        unmute_rule = get_unmute_rule(rules, build_type)
        delete_rule = get_delete_rule(rules, build_type)
        graduation_rule = get_quarantine_graduation_rule(rules, build_type)

        mute_days = get_rule_params(mute_rule, {}).get('window_days', DEFAULT_MUTE_DAYS) if mute_rule else DEFAULT_MUTE_DAYS
        unmute_days = get_rule_params(unmute_rule, {}).get('window_days', DEFAULT_UNMUTE_DAYS) if unmute_rule else DEFAULT_UNMUTE_DAYS
        delete_days = get_rule_params(delete_rule, {}).get('window_days', DEFAULT_DELETE_DAYS) if delete_rule else DEFAULT_DELETE_DAYS

        mute_rule_params = get_rule_params(mute_rule, {}) if mute_rule else {}
        unmute_rule_params = get_rule_params(unmute_rule, {}) if unmute_rule else {}
        delete_rule_params = get_rule_params(delete_rule, {}) if delete_rule else {}
        graduation_params = get_rule_params(graduation_rule, {}) if graduation_rule else {}

        logging.info(f"Rules: mute_days={mute_days}, unmute_days={unmute_days}, delete_days={delete_days}")

        logging.info("Executing single query for 7 days window...")
        all_data = execute_query(args.branch, days_window=7)
        logging.info(f"Query returned {len(all_data)} test records")

        aggregated_for_mute = aggregate_test_data(all_data, mute_days)
        aggregated_for_unmute = aggregate_test_data(all_data, unmute_days)
        aggregated_for_delete = aggregate_test_data(all_data, delete_days)
        grad_window = graduation_params.get('window_days', 1)
        aggregated_1day = aggregate_test_data(all_data, grad_window)

        logging.info(f"Aggregated data: mute={len(aggregated_for_mute)}, unmute={len(aggregated_for_unmute)}, delete={len(aggregated_for_delete)}")

        to_graduated = set()
        if quarantine_check and input_quarantine_path and os.path.exists(input_quarantine_path):
            quarantine_tests = _parse_mute_file(input_quarantine_path)
            to_graduated = get_quarantine_graduation(quarantine_tests, aggregated_1day, graduation_params)
            if to_graduated:
                updated_quarantine = quarantine_tests - to_graduated
                with open(input_quarantine_path, 'w') as f:
                    f.write('\n'.join(sorted(updated_quarantine)) + '\n')
                logging.info(f"Quarantine graduation: removed {len(to_graduated)} tests from quarantine")

        if args.mode == 'update_muted_ya':
            output_path = args.output_folder
            os.makedirs(output_path, exist_ok=True)
            logging.info(f"Creating mute files in: {output_path}")
            result = apply_and_add_mutes(
                all_data, output_path, mute_check,
                aggregated_for_mute, aggregated_for_unmute, aggregated_for_delete,
                quarantine_check=quarantine_check,
                to_graduated=to_graduated,
                mute_rule_params=mute_rule_params,
                unmute_rule_params=unmute_rule_params,
                delete_rule_params=delete_rule_params,
                output_file=getattr(args, 'output_file', None),
            )
            to_mute, to_unmute, to_delete, to_mute_debug, to_unmute_debug, to_delete_debug = result
            # Write mute decisions to YDB for traceability
            try:
                with YDBWrapper() as ydb_wrapper:
                    if ydb_wrapper.check_credentials():
                        mute_rule_id = mute_rule.get("id", "regression_flaky_mute") if mute_rule else "regression_flaky_mute"
                        unmute_rule_id = unmute_rule.get("id", "regression_stable_unmute") if unmute_rule else "regression_stable_unmute"
                        delete_rule_id = delete_rule.get("id", "regression_no_runs_delete") if delete_rule else "regression_no_runs_delete"
                        grad_rule_id = graduation_rule.get("id", "quarantine_graduation") if graduation_rule else "quarantine_graduation"
                        write_mute_decisions(
                            ydb_wrapper,
                            branch=args.branch,
                            build_type=build_type,
                            to_mute=to_mute,
                            to_unmute=to_unmute,
                            to_delete=to_delete,
                            to_graduated=to_graduated,
                            mute_rule_id=mute_rule_id,
                            unmute_rule_id=unmute_rule_id,
                            delete_rule_id=delete_rule_id,
                            graduation_rule_id=grad_rule_id,
                            to_mute_debug=to_mute_debug,
                            to_unmute_debug=to_unmute_debug,
                            to_delete_debug=to_delete_debug,
                        )
            except Exception as e:
                logging.warning(f"Failed to write mute decisions to YDB: {e}")

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
    update_muted_ya_parser.add_argument('--muted_ya_file', default=muted_ya_path, help='Path to input muted_ya.txt file')
    update_muted_ya_parser.add_argument('--quarantine_file', default=quarantine_path, help='Path to quarantine.txt (manually unmuted tests)')
    update_muted_ya_parser.add_argument('--build_type', default='relwithdebinfo', help='Build type for rule selection')
    update_muted_ya_parser.add_argument('--rules_file', help='Path to pattern_rules.yaml (default: .github/config/pattern_rules.yaml)')
    update_muted_ya_parser.add_argument('--output_file', help='Output filename for final mute file (default: new_muted_ya.txt). Use for per-build-type, e.g. new_muted_ya_asan.txt')

    create_issues_parser = subparsers.add_parser(
        'create_issues',
        help='create issues by muted_ya like files',
    )
    create_issues_parser.add_argument(
        '--file_path', default=f'{repo_path}/mute_update/to_mute.txt', required=False, help='file path'
    )
    create_issues_parser.add_argument('--branch', default='main', help='Branch to get history')
    create_issues_parser.add_argument('--close_issues', action='store_true', default=True, help='Close issues when all tests are unmuted (default: True)')

    args = parser.parse_args()

    mute_worker(args)
