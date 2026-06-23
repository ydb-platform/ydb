#!/usr/bin/env python3

import ydb
import configparser
import os
import time
import json
import requests
from datetime import datetime, timedelta
from collections import defaultdict, Counter
import urllib3
import re
import tempfile
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
from difflib import SequenceMatcher
import math
import codecs
import sys

# Отключаем предупреждения о непроверенных HTTPS запросах
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Load configuration
dir = os.path.dirname(__file__)
config = configparser.ConfigParser()
config_file_path = f"{dir}/../../config/ydb_qa_db.ini"
config.read(config_file_path)

DATABASE_ENDPOINT = config["QA_DB"]["DATABASE_ENDPOINT"]
DATABASE_PATH = config["QA_DB"]["DATABASE_PATH"]

# Anthropic API configuration
ANTHROPIC_API_URL = "https://api.eliza.yandex.net/raw/anthropic"
API_KEY = os.environ.get('ANTHROPIC_API_KEY')

DEBUG = True

def setup_logging():
    """Настраиваем логирование"""
    logging.basicConfig(
        level=logging.DEBUG,  # Изменено обратно с INFO на DEBUG для видимости всех сообщений
        format='%(asctime)s - %(levelname)s - %(message)s',
       # handlers=[
        #    logging.StreamHandler(sys.stdout) #,
            #logging.FileHandler('compatibility_report.log', encoding='utf-8')
        #]
    )
    
    # Устанавливаем уровень DEBUG для нашего модуля чтобы видеть детальную отладку
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

class ErrorPatternCache:
    """Кеш паттернов ошибок для переиспользования"""
    
    def __init__(self, cache_file="error_patterns_cache.json"):
        self.cache_file = cache_file
        self.patterns = self.load_cache()
        self.new_patterns_count = 0
    
    def load_cache(self):
        """Загружает кеш паттернов с диска"""
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    logging.debug(f"Loaded {len(data)} error patterns from cache")
                    return data
            except Exception as e:
                logging.warning(f"Failed to load error patterns cache: {e}")
        return {}
    
    def save_cache(self):
        """Сохраняет кеш паттернов на диск"""
        try:
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(self.patterns, f, ensure_ascii=False, indent=2)
            logging.debug(f"Saved {len(self.patterns)} error patterns to cache")
        except Exception as e:
            logging.warning(f"Failed to save error patterns cache: {e}")
    
    def normalize_log(self, log_text):
        """Нормализует лог для поиска паттернов - берет только полезную часть после ERROR"""
        if not log_text:
            return ""
        
        # Ищем первую строку с ERROR и берем все начиная с нее
        lines = log_text.split('\n')
        error_start_index = -1
        
        for i, line in enumerate(lines):
            if ' - ERROR - ' in line:
                error_start_index = i
                break
        
        # Если ERROR не найден, ищем другие критичные маркеры
        if error_start_index == -1:
            critical_markers = [' - EXCEPTION - ', ' - FATAL - ', ' - CRITICAL - ', 'Exception:', 'Error:']
            for marker in critical_markers:
                for i, line in enumerate(lines):
                    if marker in line:
                        error_start_index = i
                        break
                if error_start_index != -1:
                    break
        
        # Если никаких ошибок не найдено, возвращаем пустую строку
        if error_start_index == -1:
            return ""
        
        # Берем полезную часть лога (от первой ошибки до конца)
        useful_lines = lines[error_start_index:]
        
        # Фильтруем информационные строки среди полезных
        filtered_lines = []
        for line in useful_lines:
            line_stripped = line #.strip()
            if not line_stripped:
                continue
                
            # Исключаем информационные строки даже в секции ошибок
            skip_patterns = [
                r' - DEBUG - ',  # DEBUG
                r' - INFO - ',   # INFO (но не ERROR)
            ]
            
            # Проверяем, нужно ли пропустить эту строку
            should_skip = False
            for pattern in skip_patterns:
                if re.search(pattern, line_stripped):
                    should_skip = True
                    break
            
            if not should_skip:
                filtered_lines.append(line_stripped)
        
        # Если после фильтрации ничего не осталось, возвращаем пустую строку
        if not filtered_lines:
            return ""
        
        # Объединяем отфильтрованные строки
        normalized = '\n'.join(filtered_lines)
        
        # Применяем нормализацию переменных данных
        # Временные метки (все форматы)
        normalized = re.sub(r'\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}[,.]\d+', '[TIMESTAMP]', normalized)
        normalized = re.sub(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', '[TIMESTAMP]', normalized)
        
        # Пути к файлам (только очень длинные пути)
        #normalized = re.sub(r'/[a-zA-Z0-9/_.-]{40,}', '[LONG_PATH]', normalized)
        normalized = re.sub(r'([a-zA-Z0-9/_.-]*)build_root/[a-zA-Z0-9/_.-]+', '[BUILD_PATH]', normalized)
        
        # PID в разных форматах
        normalized = re.sub(r'\(pid\s+\d+\)', '(pid [PID])', normalized)
        normalized = re.sub(r'pid:?\s*\d+', 'pid:[PID]', normalized)
        
        # Все порты в YDB командах  
        normalized = re.sub(r'--grpc-port=\d+', '--grpc-port=[PORT]', normalized)
        normalized = re.sub(r'--mon-port=\d+', '--mon-port=[PORT]', normalized)
        normalized = re.sub(r'--ic-port=\d+', '--ic-port=[PORT]', normalized)
        normalized = re.sub(r'--interconnect-port=\d+', '--interconnect-port=[PORT]', normalized)
        
        # Общие порты
        normalized = re.sub(r':\d{4,5}\b', ':[PORT]', normalized)
        normalized = re.sub(r'port\s+\d+', 'port [PORT]', normalized)
        
        # Номера узлов в разных форматах
        normalized = re.sub(r'--node=\d+', '--node=[N]', normalized)
        normalized = re.sub(r'node\s*=?\s*\d+', 'node=[N]', normalized)
        normalized = re.sub(r'node\s+\d+', 'node [N]', normalized)
        
        # Хеши и идентификаторы (только длинные)
        normalized = re.sub(r'\b[a-f0-9]{16,}\b', '[HASH]', normalized)
        normalized = re.sub(r'\b[A-Fa-f0-9]{12,}\b', '[ID]', normalized)
        
        # IP адреса
        normalized = re.sub(r'\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b', '[IP]', normalized)
        normalized = re.sub(r'localhost:\d+', 'localhost:[PORT]', normalized)
        
        # Thread/процесс номера
        normalized = re.sub(r'thread-\d+', 'thread-[N]', normalized)
        normalized = re.sub(r'Thread-\d+', 'Thread-[N]', normalized)
        
        # Номера сессий/соединений
        normalized = re.sub(r'session-\d+', 'session-[ID]', normalized)
        normalized = re.sub(r'connection-\d+', 'connection-[ID]', normalized)
        
        # Временные файлы и директории
        normalized = re.sub(r'/tmp/[a-zA-Z0-9_.-]+', '/tmp/[TEMP]', normalized)
        normalized = re.sub(r'CFG_DIR_PATH="[^"]*"', 'CFG_DIR_PATH="[PATH]"', normalized)
        
        # Специфичные для YDB переменные
        normalized = re.sub(r'--log-file-name=[^\s]*', '--log-file-name=[PATH]', normalized)
        normalized = re.sub(r'--yaml-config=[^\s]*', '--yaml-config=[CONFIG]', normalized)
        
        # Убираем повторяющиеся пробелы
        #normalized = re.sub(r'\s+', ' ', normalized)
        
        return normalized.strip()
    
    def extract_error_signature(self, log_text):
        """Извлекает сигнатуру ошибки для поиска совпадений"""
        normalized = self.normalize_log(log_text)
        
        # Ищем ключевые части ошибки
        signature_parts = []
        lines = normalized.split('\n')
        
        for line in lines:
            line_lower = line.lower()
            # Добавляем в сигнатуру строки с ошибками, но без технических деталей
            if any(keyword in line_lower for keyword in [
                'error', 'exception', 'failed', 'timeout', 'assertion', 
                'unknown field', 'config', 'daemon failed'
            ]):
                # Убираем оставшиеся технические детали из сигнатуры
                clean_line = re.sub(r'\[[A-Z_]+\]', '', line)  # Убираем наши маркеры
                clean_line = re.sub(r'\s+', ' ', clean_line).strip()
                if len(clean_line) > 20:  # Только содержательные строки
                    signature_parts.append(clean_line)
                
                if len(signature_parts) >= 5:  # Максимум 5 строк в сигнатуре
                    break
        
        return '\n'.join(signature_parts)
    
    def find_matching_pattern(self, log_text, similarity_threshold=0.7):
        """Ищет подходящий паттерн в кеше"""
        signature = self.extract_error_signature(log_text)
        if not signature:
            return None
        
        best_match = None
        best_score = 0
        
        for pattern_id, pattern_data in self.patterns.items():
            stored_signature = pattern_data.get('signature', '')
            if not stored_signature:
                continue
            
            # Вычисляем схожесть сигнатур
            similarity = SequenceMatcher(None, signature, stored_signature).ratio()
            
            if similarity > similarity_threshold and similarity > best_score:
                best_score = similarity
                best_match = pattern_data
        
        if best_match:
            logging.debug(f"Found matching pattern with similarity {best_score:.2f}")
            return best_match['meaningful_error']
        
        return None
    
    def add_pattern(self, log_text, meaningful_error):
        """Добавляет новый паттерн в кеш"""
        signature = self.extract_error_signature(log_text)
        if not signature or not meaningful_error:
            return
        
        # Создаем ID паттерна
        pattern_id = hashlib.md5(signature.encode('utf-8')).hexdigest()[:12]
        
        self.patterns[pattern_id] = {
            'signature': signature,
            'meaningful_error': meaningful_error,
            'usage_count': 1,
            'created_at': datetime.now().isoformat()
        }
        
        self.new_patterns_count += 1
        logging.debug(f"Added new error pattern {pattern_id}")
    
    def clear_cache(self):
        """Очищает кеш паттернов"""
        if os.path.exists(self.cache_file):
            os.remove(self.cache_file)
            logging.info(f"Cache file {self.cache_file} removed")
        self.patterns = {}
        self.new_patterns_count = 0
        logging.info("Cache cleared")
    
    def analyze_cache_effectiveness(self):
        """Анализирует эффективность кеширования"""
        logging.info(f"=== АНАЛИЗ ЭФФЕКТИВНОСТИ КЕША ===")
        logging.info(f"Всего паттернов: {len(self.patterns)}")
        
        # Группируем паттерны по типам ошибок
        error_types = {}
        for pattern_id, pattern_data in self.patterns.items():
            signature = pattern_data.get('signature', '')
            
            # Определяем тип ошибки
            if 'unknown field' in signature.lower():
                error_type = 'CONFIG_ERROR'
            elif 'timeout' in signature.lower():
                error_type = 'TIMEOUT'
            elif 'final command' in signature.lower():
                error_type = 'STARTUP_COMMAND'
            elif 'daemon failed' in signature.lower():
                error_type = 'DAEMON_FAILED'
            else:
                error_type = 'OTHER'
            
            if error_type not in error_types:
                error_types[error_type] = []
            error_types[error_type].append(pattern_data)
        
        for error_type, patterns in error_types.items():
            total_usage = sum(p.get('usage_count', 1) for p in patterns)
            logging.info(f"{error_type}: {len(patterns)} паттернов, {total_usage} использований")
            
            # Показываем возможные дубликаты
            if len(patterns) > 3:
                logging.warning(f"Много паттернов для {error_type} - возможны дубликаты!")
                for i, pattern in enumerate(patterns[:3]):
                    logging.info(f"  Пример {i+1}: {pattern['signature'][:100]}...")

# Глобальный кеш паттернов
error_cache = ErrorPatternCache()

def save_json(data, filename):
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=json_default)
        logging.info(f"Saved {filename}")
    except Exception as e:
        logging.warning(f"Failed to save {filename}: {e}")

def save_text(data, filename):
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(data)
        logging.info(f"Saved {filename}")
    except Exception as e:
        logging.warning(f"Failed to save {filename}: {e}")

def get_compatibility_tests_data(driver, days_back=3):
    """Получает данные о compatibility тестах за последние дни"""
    logging.info(f'=== ПОЛУЧЕНИЕ ДАННЫХ ИЗ БД ===')
    logging.info(f'Запрашиваем данные compatibility тестов за последние {days_back} дней')
    start_time = time.time()
    results = []
    
    query = f"""
    SELECT 
        build_type,
        job_name,
        job_id,
        commit,
        branch,
        pull,
        run_timestamp,
        test_id,
        suite_folder,
        test_name,
        duration,
        status,
        log,
        status_description,
        owners
    FROM `test_results/test_runs_column`
    WHERE
        run_timestamp >= CurrentUtcDate() - {days_back}*Interval("P1D")

        AND String::Contains(suite_folder, 'ydb/tests/compatibility') = TRUE
        AND String::Contains(test_name, '.flake8') = FALSE
        AND String::Contains(test_name, 'chunk chunk') = FALSE
        AND String::Contains(test_name, 'chunk+chunk') = FALSE
        
        AND (branch = 'main' or branch = 'custom-compatibility-tags-19415') and build_type = 'relwithdebinfo'
        AND job_name != 'PR-check'
    ORDER BY run_timestamp DESC
    """

    # Логируем SQL запрос
    logging.debug(f'SQL запрос:\n{query}')
    
    logging.debug('Выполняем scan query...')
    scan_query = ydb.ScanQuery(query, {})
    it = driver.table_client.scan_query(scan_query)
    
    batch_count = 0
    total_rows = 0
    
    while True:
        try:
            result = next(it)
            batch_rows = len(result.result_set.rows)
            total_rows += batch_rows
            batch_count += 1
            results.extend(result.result_set.rows)
            
            logging.debug(f'Получен batch {batch_count}: {batch_rows} записей (всего: {total_rows})')
            
            # Периодически выводим прогресс для больших запросов
            if batch_count % 10 == 0:
                elapsed_current = time.time() - start_time
                logging.info(f'Прогресс: получено {total_rows} записей за {elapsed_current:.1f}с (batch #{batch_count})')
                
        except StopIteration:
            logging.debug('Scan query завершен')
            break
    
    elapsed = time.time() - start_time
    logging.info(f'Данные успешно получены: {len(results)} записей за {elapsed:.2f}с в {batch_count} batches')
    
    # Анализируем полученные данные
    if results:
        # Группируем по статусам
        status_counts = {}
        branch_counts = {}
        job_counts = {}
        oldest_timestamp = None
        newest_timestamp = None
        
        for record in results:
            # Статусы
            status = record.get('status', 'unknown')
            status_counts[status] = status_counts.get(status, 0) + 1
            
            # Ветки
            branch = record.get('branch', 'unknown')
            branch_counts[branch] = branch_counts.get(branch, 0) + 1
            
            # Job names
            job_name = record.get('job_name', 'unknown')
            job_counts[job_name] = job_counts.get(job_name, 0) + 1
            
            # Временной диапазон
            timestamp = record.get('run_timestamp')
            if timestamp:
                if isinstance(timestamp, int):
                    # Микросекунды в datetime
                    ts = datetime.utcfromtimestamp(timestamp / 1_000_000)
                elif isinstance(timestamp, float):
                    ts = datetime.utcfromtimestamp(timestamp)
                else:
                    ts = timestamp
                    
                if oldest_timestamp is None or ts < oldest_timestamp:
                    oldest_timestamp = ts
                if newest_timestamp is None or ts > newest_timestamp:
                    newest_timestamp = ts
        
        # Выводим статистику
        logging.info(f'=== СТАТИСТИКА ПОЛУЧЕННЫХ ДАННЫХ ===')
        logging.info(f'Временной диапазон: {oldest_timestamp} - {newest_timestamp}')
        
        logging.info(f'По статусам:')
        for status, count in sorted(status_counts.items(), key=lambda x: x[1], reverse=True):
            percentage = count / len(results) * 100
            logging.info(f'  {status}: {count} ({percentage:.1f}%)')
        
        logging.info(f'По веткам:')
        for branch, count in sorted(branch_counts.items(), key=lambda x: x[1], reverse=True):
            percentage = count / len(results) * 100
            logging.info(f'  {branch}: {count} ({percentage:.1f}%)')
        
        logging.info(f'По job names (топ-5):')
        sorted_jobs = sorted(job_counts.items(), key=lambda x: x[1], reverse=True)
        for job_name, count in sorted_jobs[:5]:
            percentage = count / len(results) * 100
            logging.info(f'  {job_name}: {count} ({percentage:.1f}%)')
        
        # Проверяем наличие логов у mute тестов
        mute_tests = [r for r in results if r.get('status') == 'mute']
        mute_with_logs = [r for r in mute_tests if r.get('log')]
        mute_without_logs = len(mute_tests) - len(mute_with_logs)
        
        if mute_tests:
            logging.info(f'Mute тесты: {len(mute_tests)} всего, {len(mute_with_logs)} с логами, {mute_without_logs} без логов')
        
        # Предупреждения о потенциальных проблемах
        if len(results) == 0:
            logging.warning('Получено 0 записей - проверьте фильтры запроса!')
        elif len(results) < 100:
            logging.warning(f'Получено мало записей ({len(results)}) - возможно, стоит увеличить days_back?')
            
        # Проверяем версионность данных
        sample_test_names = [f"{r.get('suite_folder', '')}/{r.get('test_name', '')}" for r in results[:10]]
        logging.debug('Примеры имен тестов для проверки парсинга версий:')
        for i, test_name in enumerate(sample_test_names, 1):
            logging.debug(f'  {i}. {test_name}')
    
    else:
        logging.warning('Получено 0 записей из БД!')
    
    return results


def parse_test_type_and_versions(test_name):
    """
    Извлекает тип теста, категорию совместимости и список версий из имени теста.
    
    Примеры:
    - test[mixed_25-1-row] -> ('mixed', 'single_version', ['25-1'])
    - test[mixed_current_and_25-1-column] -> ('mixed', 'compatibility', ['current', '25-1'])
    - test[restart_25-1_to_current-row] -> ('restart', 'compatibility', ['25-1', 'current'])
    
    Возвращает: (test_type, compatibility_category, versions_list)
    """
    match = re.search(r'\[(.*?)\]', test_name)
    if not match:
        logging.debug(f"No bracket content found in test name: {test_name}")
        return ("unknown", "single_version", ["unknown"])
    
    inside = match.group(1)
    parts = inside.split('_')
    test_type = parts[0] if parts else "unknown"
    rest = '_'.join(parts[1:])
    
    # УЛУЧШЕННЫЙ паттерн версий - поддерживает больше форматов
    version_pattern = r'(current|trunk|main|\d+-\d+(?:-\d+)?|\d+\.\d+(?:\.\d+)?|v\d+(?:\.\d+)*)'
    
    # Логируем для отладки
    logging.debug(f"Parsing test: {test_name}")
    logging.debug(f"  Test type: {test_type}")
    logging.debug(f"  Rest part: {rest}")
    
    # Проверяем на совместимость между версиями
    if '_to_' in rest:
        # Формат: restart_A_to_B или rolling_A_to_B
        versions_part = rest.split('_to_')
        logging.debug(f"  Migration test detected: {versions_part}")
        
        left_match = re.search(version_pattern, versions_part[0])
        right_match = re.search(version_pattern, versions_part[1])
        left = left_match.group(1) if left_match else 'unknown'
        right = right_match.group(1) if right_match else 'unknown'
        
        logging.debug(f"  Extracted versions: {left} -> {right}")
        if left == 'unknown' or right == 'unknown':
            logging.warning(f"Failed to parse migration versions in: {test_name}")
        
        return (test_type, "compatibility", [left, right])
    
    elif '_and_' in rest:
        # Формат: mixed_current_and_25-1
        versions_part = rest.split('_and_')
        logging.debug(f"  Compatibility test detected: {versions_part}")
        
        left_match = re.search(version_pattern, versions_part[0])
        right_match = re.search(version_pattern, versions_part[1])
        left = left_match.group(1) if left_match else 'unknown'
        right = right_match.group(1) if right_match else 'unknown'
        
        logging.debug(f"  Extracted versions: {left} & {right}")
        if left == 'unknown' or right == 'unknown':
            logging.warning(f"Failed to parse compatibility versions in: {test_name}")
        
        return (test_type, "compatibility", sorted([left, right]))
    
    else:
        # Тест одной версии: mixed_25-1-row, mixed_current-column
        version_match = re.search(version_pattern, parts[1]) if len(parts) > 1 else None
        version = version_match.group(1) if version_match else 'unknown'
        
        logging.debug(f"  Single version test: {version}")
        if version == 'unknown':
            logging.warning(f"Failed to parse single version in: {test_name} (parts: {parts})")
            
            # Дополнительная отладка - показываем все части
            if len(parts) > 1:
                logging.debug(f"    Second part for version extraction: '{parts[1]}'")
                logging.debug(f"    All available parts: {parts}")
        
        return (test_type, "single_version", [version])

def analyze_parsed_versions(test_data):
    """
    Анализирует все распарсенные версии и выводит статистику
    """
    version_stats = {
        'found_versions': [],  # Изменено с set на list для JSON сериализации
        'unknown_count': 0,
        'test_types': [],      # Изменено с set на list для JSON сериализации
        'compatibility_tests': 0,
        'single_version_tests': 0,
        'unparsed_tests': []
    }
    
    found_versions_set = set()  # Временный set для уникальности
    test_types_set = set()      # Временный set для уникальности
    
    logging.info("=== АНАЛИЗ РАСПАРСЕННЫХ ВЕРСИЙ ===")
    
    for record in test_data:
        test_name = f"{record.get('suite_folder', '')}/{record.get('test_name', '')}"
        test_type, compatibility_category, versions_list = parse_test_type_and_versions(test_name)
        
        test_types_set.add(test_type)
        
        if compatibility_category == 'single_version':
            version_stats['single_version_tests'] += 1
        else:
            version_stats['compatibility_tests'] += 1
        
        for version in versions_list:
            if version == 'unknown':
                version_stats['unknown_count'] += 1
                version_stats['unparsed_tests'].append(test_name)
            else:
                found_versions_set.add(version)
    
    # Конвертируем sets в lists для JSON сериализации
    version_stats['found_versions'] = sorted(found_versions_set)
    version_stats['test_types'] = sorted(test_types_set)
    
    # Выводим статистику
    logging.info(f"Найдено уникальных версий: {len(version_stats['found_versions'])}")
    logging.info(f"Версии: {version_stats['found_versions']}")
    logging.info(f"Типы тестов: {version_stats['test_types']}")
    logging.info(f"Тесты одной версии: {version_stats['single_version_tests']}")
    logging.info(f"Тесты совместимости: {version_stats['compatibility_tests']}")
    logging.info(f"Тесты с неопределенными версиями: {version_stats['unknown_count']}")
    
    if version_stats['unparsed_tests']:
        logging.warning(f"Тесты с неопределенными версиями ({len(version_stats['unparsed_tests'])}):")
        for test in version_stats['unparsed_tests'][:10]:  # Показываем первые 10
            logging.warning(f"  - {test}")
        if len(version_stats['unparsed_tests']) > 10:
            logging.warning(f"  ... и еще {len(version_stats['unparsed_tests']) - 10} тестов")
    
    return version_stats


def format_compatibility_context(test_type, compatibility_category, versions_list):
    """
    Форматирует контекст совместимости для отображения в отчетах.
    
    Возвращает строку вида:
    - "single: 25-1" для тестов одной версии
    - "compat: current ↔ 25-1" для тестов совместимости
    - "migration: 24-4 → 25-1" для тестов миграции
    """
    if compatibility_category == "single_version":
        return f"single: {versions_list[0] if versions_list else 'unknown'}"
    
    if len(versions_list) < 2:
        return f"unknown: {versions_list[0] if versions_list else 'unknown'}"
    
    version_a, version_b = versions_list[0], versions_list[1]
    
    if test_type in ['restart', 'rolling']:
        return f"migration: {version_a} → {version_b}"
    else:
        return f"compat: {version_a} ↔ {version_b}"


def enrich_mute_records_with_logs(test_data):
    """ЭТАП 2: Умное обогащение с кешированием паттернов ошибок"""
    return smart_error_extraction_with_cache(test_data)

def enrich_mute_records_with_logs_old(test_data):
    """ЭТАП 2: Заменяет status_description у всех mute результатов тестов из log этого запуска"""
    logging.debug("Ищем mute записи с логами для обогащения...")
    
    # Собираем все записи с mute статусом и логами
    mute_records_with_logs = []
    for record in test_data:
        if record.get('status') == 'mute' and record.get('log'):
            mute_records_with_logs.append(record)
    
    logging.debug(f"Найдено {len(mute_records_with_logs)} mute записей с логами")
    
    # Скачиваем логи параллельно
    if mute_records_with_logs:
        tasks = [(record.get('log'), f"{record.get('suite_folder', '')}/{record.get('test_name', '')}", idx) 
                for idx, record in enumerate(mute_records_with_logs)]
        
        logging.debug(f"Скачиваем {len(tasks)} логов для mute тестов...")
        
        results = {}
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_task = {executor.submit(fetch_log_content_safe, t): t for t in tasks}
            for future in as_completed(future_to_task):
                test_name, idx, content = future.result()
                if content:
                    # Используем умное извлечение содержательной информации
                    meaningful_info = extract_meaningful_error_info(content, max_length=2000)
                    results[idx] = meaningful_info
                    logging.debug(f"Скачан и обработан лог для mute теста {idx}: {len(content)} -> {len(meaningful_info)} символов")
        
        # Обновляем status_description с содержательной информацией
        updated_count = 0
        for idx, record in enumerate(mute_records_with_logs):
            if idx in results:
                record['status_description'] = results[idx]
                updated_count += 1
                logging.debug(f"Обновлен mute тест {record.get('test_name')} с содержательной информацией")
        
        logging.debug(f"Обновлено {updated_count} mute тестов с содержательной информацией об ошибках")
    
    return test_data


def filter_records_with_status_description(test_data):
    """ЭТАП 3: Исключает тесты где мы не смогли получить status_description из log"""
    logging.debug("Фильтруем записи с пустым status_description...")
    
    original_count = len(test_data)
    
    # Оставляем только записи с непустым status_description или со статусом passed/skipped
    filtered_data = []
    for record in test_data:
        status = record.get('status', '')
        status_description = record.get('status_description', '').strip()
        
        # Оставляем если:
        # 1. Статус passed/skipped (не нужно описание ошибки)
        # 2. Есть непустое status_description
        if status in ['passed', 'skipped'] or status_description:
            filtered_data.append(record)
    
    filtered_count = len(filtered_data)
    excluded_count = original_count - filtered_count
    
    logging.debug(f"Исходно записей: {original_count}")
    logging.debug(f"Отфильтровано записей: {filtered_count}")
    logging.debug(f"Исключено записей без status_description: {excluded_count}")
    
    return filtered_data


def group_by_versions_and_types(test_data):
    """ЭТАП 4: Группирует тесты по отдельным версиям и типам тестов с учетом совместимости"""
    logging.debug("Группируем данные по отдельным версиям и типам тестов с учетом совместимости...")
    
    grouped = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    
    for record in test_data:
        # Формируем полное имя теста
        test_name = f"{record.get('suite_folder', '')}/{record.get('test_name', '')}"
        
        # Парсим тип теста, категорию совместимости и список версий
        test_type, compatibility_category, versions_list = parse_test_type_and_versions(test_name)
        
        # Определяем все версии для которых нужно создать отчеты
        primary_versions = determine_primary_version_for_grouping(test_type, compatibility_category, versions_list)
        
        # Анализируем пары версий
        version_pair_info = analyze_version_pairs(test_name, test_type, compatibility_category, versions_list)
        
        # Создаем запись о запуске теста
        run_info = {
            'status': record.get('status', ''),
            'timestamp': record.get('run_timestamp'),
            'build_type': record.get('build_type', ''),
            'branch': record.get('branch', ''),
            'job_name': record.get('job_name', ''),
            'commit': record.get('commit', ''),
            'duration': record.get('duration', 0),
            'status_description': record.get('status_description', ''),
            'log': record.get('log', ''),  # Добавляем URL лога
            'test_context': format_compatibility_context(test_type, compatibility_category, versions_list),
            'compatibility_category': compatibility_category,
            'all_versions': versions_list,
            'version_pair_info': version_pair_info
        }
        
        # Добавляем тест в отчеты для ВСЕХ участвующих версий
        for primary_version in primary_versions:
            grouped[primary_version][test_type][test_name].append(run_info)
    
    # Сортируем запуски по времени (новые первыми)
    for version_data in grouped.values():
        for type_data in version_data.values():
            for test_runs in type_data.values():
                test_runs.sort(key=lambda x: x['timestamp'], reverse=True)
    
    # Подсчитываем статистику
    logging.debug(f"Сгруппировано по {len(grouped)} версиям:")
    for version, types in grouped.items():
        total_tests = sum(len(tests) for tests in types.values())
        
        # Подсчитываем тесты по категориям совместимости
        single_version_tests = 0
        compatibility_tests = 0
        
        for type_data in types.values():
            for test_runs in type_data.values():
                if test_runs:  # Если есть запуски
                    first_run = test_runs[0]
                    if first_run.get('compatibility_category') == 'single_version':
                        single_version_tests += 1
                    else:
                        compatibility_tests += 1
        
        logging.debug(f"  - {version}: {len(types)} типов тестов, {total_tests} тестов")
        logging.debug(f"    └─ Одна версия: {single_version_tests}, Совместимость: {compatibility_tests}")
    
    return dict(grouped)


def prepare_data_for_ai_analysis(grouped_data):
    """ЭТАП 5: Собирает группы для последующей передачи в AI"""
    logging.debug("Подготавливаем данные для AI анализа...")
    
    ai_data = {
        'by_version': {},
        'summary_stats': {
            'total_tests': 0,
            'failed_tests_count': 0,
            'mute_tests_count': 0,
            'success_rate': 0,
            'by_version': {}
        }
    }
    
    total_tests = 0
    failed_tests = 0
    mute_tests = 0
    total_runs = 0
    success_runs = 0
    now = datetime.utcnow()
    
    for version, types in grouped_data.items():
        ai_data['by_version'][version] = {}
        version_stats = {'total': 0, 'passed': 0, 'failure': 0, 'mute': 0}
        
        for test_type, tests in types.items():
            ai_data['by_version'][version][test_type] = {
                'failed_tests': [],
                'flaky_tests': [],
                'new_failures': [],
                'all_tests': [],
                'always_skipped_tests': []
            }
            
            for test_name, runs in tests.items():
                total_tests += 1
                version_stats['total'] += 1
                
                # Берем последние 10 запусков для анализа
                recent_runs = runs[:10]
                statuses = [run['status'] for run in recent_runs]
                
                fail_count = statuses.count('failure') + statuses.count('mute')
                mute_count = statuses.count('mute')
                success_count = statuses.count('passed')
                
                total_runs += len(statuses)
                success_runs += statuses.count('passed')
                
                # Статистика по версиям
                if 'passed' in statuses:
                    version_stats['passed'] += 1
                if 'failure' in statuses:
                    version_stats['failure'] += 1
                if 'mute' in statuses:
                    version_stats['mute'] += 1
                
                # Общая статистика
                if any(s == 'mute' for s in statuses):
                    mute_tests += 1
                if any(s in ['failure', 'mute'] for s in statuses):
                    failed_tests += 1
                
                # Определяем время последнего запуска
                last_run = recent_runs[0] if recent_runs else None
                last_time = None
                if last_run and last_run.get('timestamp'):
                    try:
                        ts = last_run['timestamp']
                        if isinstance(ts, int):
                            last_time = datetime.utcfromtimestamp(ts / 1_000_000)
                        elif isinstance(ts, float):
                            last_time = datetime.utcfromtimestamp(ts)
                        elif isinstance(ts, str):
                            last_time = datetime.fromisoformat(ts)
                        else:
                            last_time = ts
                    except Exception:
                        last_time = None
                
                # Находим первую ошибку (из первого failed/mute run)
                error_description = ""
                log_url = ""
                for run in recent_runs:
                    if run.get('status') in ['failure', 'mute'] and run.get('status_description'):
                        error_description = run.get('status_description', '')
                        log_url = run.get('log', '')  # Получаем URL лога
                        break
                
                test_info = {
                    'name': test_name,
                    'recent_runs': recent_runs[:5],  # Сохраняем только 5 последних для отчета
                    'fail_rate': fail_count / len(statuses) if statuses else 0,
                    'latest_status': statuses[0] if statuses else 'unknown',
                    'last_time': last_time,
                    'version': version,
                    'type': test_type,
                    'error_description': error_description,
                    'log_url': log_url  # Добавляем URL лога
                }
                
                ai_data['by_version'][version][test_type]['all_tests'].append(test_info)
                
                # Классифицируем тесты
                if fail_count > 0:
                    # Новые падения (последний статус failed/mute, но были passed)
                    if statuses[0] in ['failure', 'mute'] and len(statuses) > 1 and 'passed' in statuses[1:3]:
                        ai_data['by_version'][version][test_type]['new_failures'].append(test_info)
                    # Нестабильные (есть и падения и успехи)
                    elif fail_count > 0 and success_count > 0:
                        ai_data['by_version'][version][test_type]['flaky_tests'].append(test_info)
                    # Постоянно падающие (80%+ падений)
                    elif fail_count >= len(statuses) * 0.8:
                        ai_data['by_version'][version][test_type]['failed_tests'].append(test_info)
                
                # Всегда skipped
                if all(s == 'skipped' for s in statuses) and statuses:
                    ai_data['by_version'][version][test_type]['always_skipped_tests'].append(test_info)
        
        ai_data['summary_stats']['by_version'][version] = version_stats
    
    # Общая статистика
    ai_data['summary_stats']['total_tests'] = total_tests
    ai_data['summary_stats']['failed_tests_count'] = failed_tests
    ai_data['summary_stats']['mute_tests_count'] = mute_tests
    ai_data['summary_stats']['success_rate'] = (success_runs / total_runs * 100) if total_runs else 0
    
    logging.debug(f"Подготовлены данные для AI:")
    logging.debug(f"  - Всего тестов: {total_tests}")
    logging.debug(f"  - Упавших тестов: {failed_tests}")
    logging.debug(f"  - Mute тестов: {mute_tests}")
    logging.debug(f"  - Успешность: {ai_data['summary_stats']['success_rate']:.1f}%")
    
    return ai_data


def get_available_models():
    """Получает список доступных моделей"""
    if not API_KEY:
        return []
    
    headers = {
        'Content-Type': 'application/json',
        'x-api-key': API_KEY,
        'anthropic-version': '2023-06-01'
    }
    
    try:
        # Пробуем получить список моделей
        response = requests.get(
            f"{ANTHROPIC_API_URL}/v1/models",
            headers=headers,
            timeout=30,
            verify=False
        )
        
        if response.status_code == 200:
            models_data = response.json()
            logging.debug(f"Available models: {models_data}")
            return models_data
        else:
            logging.debug(f"Models API Error: {response.status_code} - {response.text}")
            return []
            
    except Exception as e:
        logging.debug(f"Error getting models: {e}")
        return []


def fetch_log_content(log_url):
    try:
        with requests.get(log_url, timeout=20) as r:
            r.raise_for_status()
            return r.text
    except Exception as e:
        logging.debug(f"Failed to fetch log {log_url}: {e}")
        return None


def clean_log_text(text, max_length=20000):
    if not isinstance(text, str):
        try:
            text = text.decode('utf-8', errors='replace')
        except Exception:
            text = str(text)
    # Удаляем неотображаемые символы, кроме \n, \r, \t и печатных
    text = re.sub(r'[^\x09\x0A\x0D\x20-\x7Eа-яА-ЯёЁa-zA-Z0-9.,:;!?@#%&*()\[\]{}<>/\\|\-_=+"\'`~]', '', text)
    if len(text) > max_length:
        text = text[:max_length] + '\n...\n[truncated]'
    return text


def extract_meaningful_error_info(text, max_length=1000, log_url=None):
    """Извлекает содержательную информацию об ошибке, используя ту же логику что и normalize_log"""
    if not text:
        if log_url:
            return f"No clear error found in log. [View full log]({log_url})"
        return "No clear error found in log"
    
    if not isinstance(text, str):
        try:
            text = text.decode('utf-8', errors='replace')
        except Exception:
            text = str(text)
    
    # Проверка наличия важных маркеров в исходном логе
    has_verify_info = bool(re.search(r'VERIFY|verify', text, re.IGNORECASE))
    has_critical_info = bool(re.search(r'CRITICAL ERROR|FATAL ERROR|PANIC', text, re.IGNORECASE))
    
    # УЛУЧШЕННАЯ ЛОГИКА: Обрабатываем экранированные строки типа std_err:b'...'
    decoded_text = text
    
    # НОВАЯ ЛОГИКА: Обрабатываем failure тесты где ';;' уже заменено на '\n'
    # Если текст содержит структурированные ошибки, разделенные переносами строк
    if '\n' in decoded_text and not any(marker in decoded_text for marker in [' - ERROR - ', 'DECODED_STDERR:', 'DECODED_STDOUT:']):
        # Это может быть failure тест с нормализованным описанием
        # Ищем содержательные строки среди разделенных переносами
        lines = decoded_text.split('\n')
        meaningful_lines = []
        
        for line in lines:
            line_stripped = line.strip()
            if not line_stripped:
                continue
                
            # Ищем строки с важной информацией об ошибках
            if any(marker in line_stripped.lower() for marker in [
                'error', 'exception', 'failed', 'timeout', 'assertion', 'abort',
                'daemon failed', 'severaldaemonerrors', 'verify failed', 'memory limit',
                'type mismatch', 'unknown field', 'requirement', 'cannot kill'
            ]):
                meaningful_lines.append(line_stripped)
                
            # Ограничиваем количество строк
            if len(meaningful_lines) >= 5:
                break
        
        if meaningful_lines:
            # Если нашли содержательные строки, используем их
            result = '\n'.join(meaningful_lines)
            
            # Применяем базовую нормализацию
            result = re.sub(r'([a-zA-Z0-9/_.-]*)build_root/[a-zA-Z0-9/_.-]+', '[BUILD_PATH]', result)
            result = re.sub(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d+', '[TIMESTAMP]', result)
            result = re.sub(r':\d{4,5}\b', ':[PORT]', result)
            result = re.sub(r'--node=\d+', '--node=[N]', result)
            
            # Добавляем ссылку на лог если есть
            if log_url:
                result = result.strip() + f"\n[View full log]({log_url})"
            
            return result.strip()
    
    # Улучшенный regex который правильно обрабатывает экранированные кавычки
    # Ищем std_err:b' и берем все до неэкранированной закрывающей кавычки
    std_err_pattern = r"std_err:b'((?:[^'\\\\]|\\\\.)*)'"
    std_err_match = re.search(std_err_pattern, decoded_text, re.DOTALL)
    if std_err_match:
        encoded_content = std_err_match.group(1)
        try:
            # Заменяем \\\\n на \\n, \\\\r на \\r, \\\\' на ', \\\\t на \\t
            decoded_content = encoded_content.replace('\\\\n', '\\n').replace('\\\\r', '\\r').replace("\\\\'", "'").replace('\\\\t', '\\t')
            
            # Убираем escape-последовательности ANSI (типа \\x1b[K)
            decoded_content = re.sub(r'\\\\x[0-9a-fA-F]{2}', '', decoded_content)
            
            # Заменяем исходную экранированную строку на декодированную
            decoded_text = decoded_text.replace(std_err_match.group(0), f"DECODED_STDERR:\\n{decoded_content}")
            
        except Exception as e:
            # Если не удалось декодировать, оставляем как есть
            pass
    
    # Также ищем std_out
    std_out_pattern = r"std_out:b'((?:[^'\\\\]|\\\\.)*)'"
    std_out_match = re.search(std_out_pattern, decoded_text, re.DOTALL)
    if std_out_match:
        encoded_content = std_out_match.group(1)
        try:
            decoded_content = encoded_content.replace('\\\\n', '\\n').replace('\\\\r', '\\r').replace("\\\\'", "'").replace('\\\\t', '\\t')
            decoded_content = re.sub(r'\\\\x[0-9a-fA-F]{2}', '', decoded_content)
            decoded_text = decoded_text.replace(std_out_match.group(0), f"DECODED_STDOUT:\\n{decoded_content}")
        except Exception as e:
            pass
    
    # Используем ту же логику что и в normalize_log для поиска ошибок
    lines = decoded_text.split('\n')
    error_start_index = -1
    
    # УЛУЧШЕННЫЙ ПОИСК: Ищем ERROR строки с более широким набором паттернов
    for i, line in enumerate(lines):
        # Ищем различные варианты ERROR строк + VERIFY ошибки
        if any(pattern in line for pattern in [
            ' - ERROR - ',           # Стандартный ERROR
            'ERROR -',               # Вариант без пробелов
            'ExecutionError:',       # Прямая ошибка выполнения
            'yatest.common.process.ExecutionError:', # Специфичная ошибка yatest
            'VERIFY failed',         # КРИТИЧНО: Memory verification ошибки
        ]):
            error_start_index = i
            break
    
    # Если ERROR не найден, ищем другие критичные маркеры
    if error_start_index == -1:
        critical_markers = [
            # ВЫСОКИЙ ПРИОРИТЕТ: Daemon crashes и системные ошибки
            'SeveralDaemonErrors:',  # Множественные ошибки daemon'ов
            'Daemon failed with message:',  # Ошибки daemon'ов
            'Unexpectedly finished before stop',  # Неожиданное завершение
            'Process exit_code = -',  # Crash с отрицательным exit code
            'exit_code = -11',       # SIGSEGV
            'exit_code = -6',        # SIGABRT
            'Not freed 0x',          # Memory leaks
            
            # Конкретные ошибки с высоким приоритетом
            'Function.*type mismatch',  # UDF type mismatch ошибки
            'VERIFY failed',  # Memory verification ошибки
            'Unknown node in OLAP comparison compiler',  # OLAP ошибки
            'Mkql memory limit exceeded',  # Memory limit ошибки
            'KiKiMR start failed',  # Startup ошибки
            'Cannot kill a stopped process',  # Process ошибки
            'bootstrap controller timeout',  # Controller timeout
            'requirement.*failed',  # Assertion ошибки
            'Request timeout.*exceeded',  # Request timeout
            'Command.*has failed with code',  # Command execution failures
            
            # Общие маркеры (с меньшим приоритетом)
            ' - EXCEPTION - ', ' - FATAL - ', ' - CRITICAL - ', 'Exception:', 'Error:', 
            'unknown field', 'timeout', 'assertion',
            'DECODED_STDERR:', 'DECODED_STDOUT:',  # Добавляем новые маркеры
            'VerifyDebug',           # Дополнительные VERIFY детали
            'GRpc memory quota',
            'Command.*has failed with code',
            'yatest.common.process.ExecutionError:',
            r'^E\s+',  # Pytest error lines (начинающиеся с "E   ")
            'Errors:',
            'std_err:',
            'std_out:',
            'failed with code',
            r'raise ExecutionError',  # Строки с raise ExecutionError
        ]
        
        for marker in critical_markers:
            for i, line in enumerate(lines):
                # Используем regex для более точного поиска
                if re.search(marker, line, re.IGNORECASE):
                    error_start_index = i
                    break
            if error_start_index != -1:
                break
    
    # Если никаких ошибок не найдено, возвращаем сообщение со ссылкой на лог
    if error_start_index == -1:
        if log_url:
            return f"No clear error found in log. [View full log]({log_url})"
        return "No clear error found in log"
    
    # УЛУЧШЕННАЯ ЛОГИКА: Берем контекст вокруг ошибки
    # Для daemon crashes важны строки ПОСЛЕ ошибки, где содержатся детали
    context_start = max(0, error_start_index - 1)  # 1 строка до ошибки
    
    # Для daemon crashes берем больше контекста
    if any(marker in lines[error_start_index].lower() for marker in [
        'severaldaemonerrors', 'daemon failed', 'unexpectedly finished', 'exit_code = -'
    ]):
        context_end = min(len(lines), error_start_index + 30)  # До 30 строк для daemon crashes
    else:
        context_end = min(len(lines), error_start_index + 20)  # До 20 строк для остальных
    
    useful_lines = lines[context_start:context_end]
    
    # Фильтруем информационные строки среди полезных, но сохраняем daemon crash детали
    filtered_lines = []
    for line in useful_lines:
        line_stripped = line.strip()
        if not line_stripped:
            continue
            
        # НЕ исключаем строки с важной информацией об ошибках
        skip_patterns = [
            r' - DEBUG - ',  # DEBUG
            r' - INFO - (?!.*ExecutionError)',   # INFO, но НЕ если содержит ExecutionError
        ]
        
        # Проверяем, нужно ли пропустить эту строку
        should_skip = False
        for pattern in skip_patterns:
            if re.search(pattern, line_stripped):
                should_skip = True
                break
        
        # ВСЕГДА включаем строки с важной информацией об ошибках
        important_error_markers = [
            'ExecutionError:',
            'SeveralDaemonErrors:',
            'Daemon failed with message:',
            'Unexpectedly finished',
            'Process exit_code',
            'exit_code = -',
            'Not freed 0x',
            'VERIFY failed',         # КРИТИЧНО: Memory verification ошибки
            'VerifyDebug',           # Дополнительные VERIFY детали
            'GRpc memory quota',
            'Command.*has failed with code',
            'yatest.common.process.ExecutionError:',
            r'^E\s+',  # Pytest error lines (начинающиеся с "E   ")
            'Errors:',
            'std_err:',
            'std_out:',
            'failed with code',
            r'raise ExecutionError',  # Строки с raise ExecutionError
        ]
        
        has_important_error = any(re.search(marker, line_stripped, re.IGNORECASE) for marker in important_error_markers)
        
        if not should_skip or has_important_error:
            filtered_lines.append(line_stripped)
    
    # Если после фильтрации ничего не осталось, берем исходные строки
    if not filtered_lines:
        filtered_lines = [line.strip() for line in useful_lines if line.strip()][:8]
    
    # ПРИОРИТИЗИРУЕМ ВАЖНЫЕ СТРОКИ: Выносим критичные ошибки в начало
    priority_lines = []
    regular_lines = []
    
    for line in filtered_lines:
        # Высокий приоритет для daemon crashes и системных ошибок + VERIFY
        if any(marker in line.lower() for marker in [
            'severaldaemonerrors:', 'daemon failed', 'unexpectedly finished', 
            'exit_code = -', 'not freed 0x', 'executionerror:', 'verify failed'
        ]):
            priority_lines.append(line)
        elif (line.strip().startswith('E   ') and 
              ('ExecutionError:' in line or 'Command' in line or 'Errors:' in line)):
            priority_lines.append(line)
        else:
            regular_lines.append(line)
    
    # Объединяем: сначала приоритетные, потом обычные, берем до 20 строк для daemon crashes
    reordered_lines = priority_lines + regular_lines
    max_lines = 20 if any('daemon' in line.lower() or 'exit_code' in line.lower() for line in priority_lines) else 15
    result = '\n'.join(reordered_lines[:max_lines])
    
    # Применяем нормализацию переменных данных (как в normalize_log)
    # Временные метки
    result = re.sub(r'\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}[,.]\d+', '[TIMESTAMP]', result)
    result = re.sub(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', '[TIMESTAMP]', result)
    
    # Build пути
    result = re.sub(r'([a-zA-Z0-9/_.-]*)build_root/[a-zA-Z0-9/_.-]+', '[BUILD_PATH]', result)
    result = re.sub(r'/home/runner/\.ya/build/[a-zA-Z0-9/_.-]{30,}', '[BUILD_PATH]', result)
    
    # PID в разных форматах
    result = re.sub(r'\(pid\s+\d+\)', '(pid [PID])', result)
    result = re.sub(r'pid:?\s*\d+', 'pid:[PID]', result)
    
    # Порты
    result = re.sub(r'--grpc-port=\d+', '--grpc-port=[PORT]', result)
    result = re.sub(r'--mon-port=\d+', '--mon-port=[PORT]', result)
    result = re.sub(r':\d{4,5}\b', ':[PORT]', result)
    
    # Номера узлов
    result = re.sub(r'--node=\d+', '--node=[N]', result)
    result = re.sub(r'node\s*=?\s*\d+', 'node=[N]', result)
    result = re.sub(r'node\s+\d+', 'node [N]', result)
    
    # Хеши и идентификаторы (только длинные)
    result = re.sub(r'\b[a-f0-9]{16,}\b', '[HASH]', result)
    result = re.sub(r'\b[A-Fa-f0-9]{12,}\b', '[ID]', result)
    
    # IP адреса
    result = re.sub(r'\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b', '[IP]', result)
    result = re.sub(r'localhost:\d+', 'localhost:[PORT]', result)
    
    # Thread/процесс номера
    result = re.sub(r'thread-\d+', 'thread-[N]', result)
    result = re.sub(r'Thread-\d+', 'Thread-[N]', result)
    
    # Номера сессий/соединений
    result = re.sub(r'session-\d+', 'session-[ID]', result)
    result = re.sub(r'connection-\d+', 'connection-[ID]', result)
    
    # Временные файлы и директории
    result = re.sub(r'/tmp/[a-zA-Z0-9_.-]+', '/tmp/[TEMP]', result)
    result = re.sub(r'CFG_DIR_PATH="[^"]*"', 'CFG_DIR_PATH="[PATH]"', result)
    
    # Специфичные для YDB переменные
    result = re.sub(r'--log-file-name=[^\s]*', '--log-file-name=[PATH]', result)
    result = re.sub(r'--yaml-config=[^\s]*', '--yaml-config=[CONFIG]', result)
    
    # Убираем лишние пробелы и пунктуацию
    #normalized = re.sub(r'\s+', ' ', normalized)
    
    # Если результат пустой, возвращаем сообщение со ссылкой на лог
    if not result.strip():
        if log_url:
            return f"No meaningful error found. [View full log]({log_url})"
        return "No meaningful error found"
    
    # Проверка сохранения важной информации после базовой обработки
    verify_preserved = has_verify_info and bool(re.search(r'VERIFY|verify', result, re.IGNORECASE))
    critical_preserved = has_critical_info and bool(re.search(r'CRITICAL ERROR|FATAL ERROR|PANIC', result, re.IGNORECASE))
    
    # Фолбек на AI обработку, если потеряна важная информация
    if (has_verify_info and not verify_preserved) or (has_critical_info and not critical_preserved):
        logging.warning(f"Important information lost in basic processing. Falling back to AI processing.")
        
        # Используем AI для обработки лога, если потеряна важная информация
        try:
            ai_processed_error = process_single_log_with_ai(text, max_length)
            
            # Проверяем, сохранил ли AI важную информацию
            ai_verify_preserved = has_verify_info and bool(re.search(r'VERIFY|verify', ai_processed_error, re.IGNORECASE))
            ai_critical_preserved = has_critical_info and bool(re.search(r'CRITICAL ERROR|FATAL ERROR|PANIC', ai_processed_error, re.IGNORECASE))
            
            if (ai_verify_preserved or not has_verify_info) and (ai_critical_preserved or not has_critical_info):
                logging.info("AI processing successfully preserved important information.")
                result = ai_processed_error
            else:
                logging.warning("AI processing also lost important information. Using combined approach.")
                # Если AI тоже потерял информацию, комбинируем результаты
                result = f"{ai_processed_error}\n\nAdditional context: {result[:max_length // 2]}"
        except Exception as e:
            logging.error(f"Error during AI fallback processing: {e}")
            # В случае ошибки при обработке AI, используем базовую обработку
    
    # ВСЕГДА добавляем ссылку на лог в конце для валидации
    if log_url:
        result = result.strip() + f"\n[View full log]({log_url})"
    
    return result.strip()


def process_single_log_with_ai(log_text, max_length=1000):
    """
    Обрабатывает отдельный лог с помощью AI для извлечения наиболее важной информации.
    Особое внимание уделяется сохранению VERIFY информации и критических ошибок.
    """
    # Проверяем наличие важных маркеров
    has_verify_info = bool(re.search(r'VERIFY|verify', log_text, re.IGNORECASE))
    has_critical_info = bool(re.search(r'CRITICAL ERROR|FATAL ERROR|PANIC', log_text, re.IGNORECASE))
    
    # Если есть VERIFY или критические ошибки, извлекаем эти строки для включения в промпт
    important_lines = []
    if has_verify_info or has_critical_info:
        for line in log_text.split('\n'):
            if 'VERIFY failed' in line or 'requirement' in line and 'failed' in line:
                important_lines.append(line.strip())
            elif any(marker in line for marker in ['CRITICAL ERROR', 'FATAL ERROR', 'PANIC']):
                important_lines.append(line.strip())
            if len(important_lines) >= 10:  # Ограничиваем количество строк
                break
    
    # Ограничиваем размер лога для обработки AI
    log_for_ai = log_text
    if len(log_text) > 15000:
        # Если есть важные строки, добавляем их в начало сокращенного лога
        if important_lines:
            important_section = "\n".join(important_lines)
            # Берем начало лога + важные строки + конец лога
            log_for_ai = log_text[:7000] + "\n\n### IMPORTANT LINES ###\n" + important_section + "\n\n" + log_text[-7000:]
        else:
            log_for_ai = log_text[:15000]  # Берем только первые 15K символов для AI
    
    # Улучшенный промпт с акцентом на сохранение VERIFY информации
    prompt = """
    Extract the most important error information from this log. 
    Focus on preserving VERIFY information, critical errors, and error causes.
    
    🔥 CRITICAL: If the log contains "VERIFY failed" or "requirement" with "failed", ALWAYS include 
    the FULL VERIFY information with ALL numbers and details in your response.
    
    Provide a concise summary (max 1000 characters) that includes:
    1. The main error message
    2. Any VERIFY information - MUST INCLUDE FULL DETAILS (numbers, requirements, etc.)
    3. Root cause if identifiable
    4. Context (where the error occurred)
    
    EXAMPLES OF GOOD RESPONSES:
    - "VERIFY failed (Z): Allocated: 41981872, Freed: 41975776, Peak: 6726904. VerifyDebug(): requirement GetUsage() == 0 failed. Daemon failed with message: Unexpectedly finished before stop."
    - "Function 'DateTime2.Format' type mismatch: expected Type (Callable) with DateTime2.TM64, actual Type (Callable) with DateTime2.TM"
    
    Log:
    """
    
    try:
        # Вызываем AI для обработки лога
        response = call_single_ai_request(prompt, {"log": log_for_ai})
        
        # Обрабатываем ответ AI
        if response and isinstance(response, str):
            # Проверяем, сохранил ли AI важную информацию
            ai_verify_preserved = has_verify_info and bool(re.search(r'VERIFY|verify', response, re.IGNORECASE))
            ai_critical_preserved = has_critical_info and bool(re.search(r'CRITICAL ERROR|FATAL ERROR|PANIC', response, re.IGNORECASE))
            
            # Если AI не сохранил важную информацию, добавляем ее принудительно
            if has_verify_info and not ai_verify_preserved and important_lines:
                verify_lines = [line for line in important_lines if 'VERIFY' in line or ('requirement' in line and 'failed' in line)]
                if verify_lines:
                    verify_info = " ".join(verify_lines)
                    response = f"{response}\n\nVERIFY information: {verify_info}"
                    logging.info(f"Added missing VERIFY information to AI response")
            
            # Ограничиваем длину ответа
            if len(response) > max_length:
                response = response[:max_length]
            
            return response.strip()
        else:
            logging.warning("AI returned invalid response format")
            
            # Если AI не сработал, но есть важная информация, возвращаем ее напрямую
            if important_lines:
                return "Critical information: " + " ".join(important_lines)
            
            return extract_error_essence(log_text, max_length)
    except Exception as e:
        logging.error(f"Error calling AI for log processing: {e}")
        
        # В случае ошибки, если есть важная информация, возвращаем ее напрямую
        if important_lines:
            return "Critical information: " + " ".join(important_lines)
        
        return extract_error_essence(log_text, max_length)


def smart_compress_data_for_ai(data, target_token_limit=180000):
    """Умное сжатие данных с сохранением содержательности"""
    if not isinstance(data, dict):
        return data
    
    # Оцениваем текущий размер
    current_size = estimate_token_count(data)
    
    if current_size <= target_token_limit:
        return data  # Сжатие не нужно
    
    logging.debug(f"Smart compression: current {current_size} tokens, target {target_token_limit}")
    
    compressed = {}
    
    for key, value in data.items():
        if key == 'by_version':
            compressed[key] = {}
            for version, version_data in value.items():
                compressed[key][version] = {}
                for test_type, type_data in version_data.items():
                    compressed[key][version][test_type] = {}
                    for category, tests in type_data.items():
                        if isinstance(tests, list):
                            compressed_tests = []
                            for test in tests:
                                compressed_test = {
                                    'name': test.get('name', ''),
                                    'latest_status': test.get('latest_status', ''),
                                    'version': test.get('version', ''),
                                    'type': test.get('type', '')
                                }
                                
                                # Умное сжатие error_description
                                error_desc = test.get('error_description', '')
                                if error_desc:
                                    # Определяем размер на основе важности теста
                                    if test.get('latest_status') in ['failure', 'mute']:
                                        max_error_length = 1500  # Больше места для важных ошибок
                                    else:
                                        max_error_length = 800   # Меньше для остальных
                                    
                                    compressed_test['error_description'] = extract_meaningful_error_info(
                                        error_desc, max_error_length
                                    )
                                
                                # Сохраняем только ключевые поля из recent_runs
                                if test.get('recent_runs'):
                                    compressed_test['recent_statuses'] = [
                                        run.get('status', '') for run in test['recent_runs'][:3]
                                    ]
                                
                                compressed_tests.append(compressed_test)
                            
                            compressed[key][version][test_type][category] = compressed_tests
                        else:
                            compressed[key][version][test_type][category] = tests
        else:
            # Остальные ключи копируем как есть
            compressed[key] = value
    
    # Проверяем результат
    new_size = estimate_token_count(compressed)
    compression_ratio = (1 - new_size / current_size) * 100
    
    logging.debug(f"Smart compression result: {new_size} tokens ({compression_ratio:.1f}% reduction)")
    
    return compressed


def aggressively_clean_error_text(text, max_length=2000):
    """Агрессивно очищает текст ошибки для экономии токенов (УСТАРЕВШАЯ - используйте extract_meaningful_error_info)"""
    if not text:
        return ""
    
    # Используем новую умную функцию
    return extract_meaningful_error_info(text, max_length)


def extract_error_essence(text, max_length=500):
    """Извлекает суть ошибки - самые важные строки"""
    if not text:
        return ""
    
    lines = text.split('\n')
    important_lines = []
    
    # Ключевые слова для поиска важных строк
    error_keywords = [
        'error', 'exception', 'failed', 'timeout', 'assertion', 'abort',
        'panic', 'fatal', 'critical', 'denied', 'refused', 'invalid',
        'ошибка', 'исключение', 'провал', 'таймаут', 'отказ'
    ]
    
    for line in lines:
        line_lower = line.lower()
        if any(keyword in line_lower for keyword in error_keywords):
            important_lines.append(line.strip())
            if len(important_lines) >= 5:  # Максимум 5 важных строк
                break
    
    # Если не нашли важных строк, берем первые непустые
    if not important_lines:
        for line in lines:
            if line.strip():
                important_lines.append(line.strip())
                if len(important_lines) >= 3:
                    break
    
    result = '\n'.join(important_lines)
    if len(result) > max_length:
        result = result[:max_length] + '[...]'
    
    return result


def compress_data_for_ai(data, compression_level='medium'):
    """Сжимает данные для AI с разными уровнями агрессивности"""
    if not isinstance(data, dict):
        return data
    
    compressed = {}
    
    for key, value in data.items():
        if key == 'by_version':
            compressed[key] = {}
            for version, version_data in value.items():
                compressed[key][version] = {}
                for test_type, type_data in version_data.items():
                    compressed[key][version][test_type] = {}
                    for category, tests in type_data.items():
                        if category == 'all_tests':
                            # Для all_tests применяем сжатие
                            compressed_tests = []
                            for test in tests:
                                compressed_test = {
                                    'name': test.get('name', ''),
                                    'latest_status': test.get('latest_status', ''),
                                    'version': test.get('version', ''),
                                    'type': test.get('type', '')
                                }
                                
                                # Сжимаем error_description в зависимости от уровня
                                error_desc = test.get('error_description', '')
                                if compression_level == 'light':
                                    compressed_test['error_description'] = aggressively_clean_error_text(error_desc, 3000)
                                elif compression_level == 'medium':
                                    compressed_test['error_description'] = aggressively_clean_error_text(error_desc, 1500)
                                elif compression_level == 'aggressive':
                                    compressed_test['error_description'] = extract_error_essence(error_desc, 500)
                                
                                # Убираем recent_runs для экономии места
                                if compression_level in ['medium', 'aggressive']:
                                    pass  # Не добавляем recent_runs
                                else:
                                    compressed_test['recent_runs'] = test.get('recent_runs', [])[:2]  # Только 2 последних
                                
                                compressed_tests.append(compressed_test)
                            
                            compressed[key][version][test_type][category] = compressed_tests
                        else:
                            # Для других категорий (failed_tests, flaky_tests, etc.) тоже сжимаем
                            compressed[key][version][test_type][category] = []
                            for test in tests:
                                compressed_test = {
                                    'name': test.get('name', ''),
                                    'latest_status': test.get('latest_status', ''),
                                    'error_description': extract_error_essence(test.get('error_description', ''), 300)
                                }
                                compressed[key][version][test_type][category].append(compressed_test)
        else:
            # Остальные ключи копируем как есть
            compressed[key] = value
    
    return compressed


def estimate_token_count(data):
    """Примерная оценка количества токенов (1 токен ≈ 4 символа для английского)"""
    text = json.dumps(data, ensure_ascii=False, default=json_default)
    # Для русского текста коэффициент может быть выше
    return len(text) // 3  # Консервативная оценка


def get_error_text_for_test(test):
    if test.get('latest_status') != 'passed' and test.get('log'):
        log_content = fetch_log_content(test['log'])
        if log_content:
            cleaned = clean_log_text(log_content)
            # Логируем первые 200 символов лога для отладки
            logging.debug(f"[DEBUG] log for {test.get('log')}:\n{cleaned[:200]}\n{'-'*40}")
            return cleaned
    return clean_log_text(test.get('error_description') or '')


def json_default(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


def is_unstable_last_24h(test_runs, now=None):
    if not now:
        now = datetime.utcnow()
    for run in test_runs:
        ts = run.get('timestamp')
        if isinstance(ts, int):
            run_time = datetime.utcfromtimestamp(ts / 1_000_000)
        elif isinstance(ts, float):
            run_time = datetime.utcfromtimestamp(ts)
        elif isinstance(ts, str):
            try:
                run_time = datetime.fromisoformat(ts)
            except Exception:
                continue
        else:
            continue
        if (now - run_time) <= timedelta(days=1) and run.get('status') != 'passed':
            return True
    return False


def fetch_log_content_safe(args):
    log_url, test_name, run_idx = args
    try:
        content = fetch_log_content(log_url)
        return (test_name, run_idx, content)
    except Exception as e:
        logging.warning(f"Failed to fetch log for {test_name} run {run_idx}: {e}")
        return (test_name, run_idx, None)


def fetch_log_content_safe_simple(args):
    """Упрощенная версия для нового подхода"""
    log_url, idx = args
    try:
        content = fetch_log_content(log_url)
        return (idx, content)
    except Exception as e:
        logging.warning(f"Failed to fetch log {idx}: {e}")
        return (idx, None)


def needs_ai_processing(basic_result):
    """Определяет, нужна ли AI обработка для результата базовой обработки"""
    if not basic_result or len(basic_result) < 100:
        return True  # Слишком мало информации
    
    # Проверяем качество базовой обработки
    quality_indicators = [
        'CONFIG ERRORS:' in basic_result,
        'STARTUP ERRORS:' in basic_result, 
        'ERRORS:' in basic_result,
        'TIMEOUTS:' in basic_result,
        'EXCEPTIONS:' in basic_result
    ]
    
    if sum(quality_indicators) >= 1:
        # Базовая обработка нашла структурированную информацию
        return len(basic_result) > 1200  # AI только если результат слишком длинный
    
    # Базовая обработка не справилась со структурированием
    return True


def process_error_batch_with_ai(batch_data):
    """Обрабатывает батч ошибок через AI"""
    
    # ДЕТАЛЬНОЕ ЛОГИРОВАНИЕ входных данных
    logging.debug("🤖 AI обработка начата")
    for key, data in batch_data.items():
        if isinstance(data, dict):
            original_log = data.get('original_log', '')
            basic_processing = data.get('basic_processing', '')
            has_verify_original = 'VERIFY' in original_log
            has_verify_basic = 'VERIFY' in basic_processing
            
            logging.debug(f"  📋 {key}: original_log длина={len(original_log)}, VERIFY={'✅' if has_verify_original else '❌'}")
            logging.debug(f"  📋 {key}: basic_processing длина={len(basic_processing)}, VERIFY={'✅' if has_verify_basic else '❌'}")
            
            if has_verify_original or has_verify_basic:
                logging.info(f"🔍 VERIFY DEBUGGING - AI получил данные с VERIFY:")
                if has_verify_original:
                    verify_lines = [line.strip() for line in original_log.split('\n') if 'VERIFY failed' in line]
                    logging.info(f"  📋 VERIFY строки в original_log: {verify_lines[:2]}")
                if has_verify_basic:
                    verify_lines = [line.strip() for line in basic_processing.split('\n') if 'VERIFY failed' in line]
                    logging.info(f"  📋 VERIFY строки в basic_processing: {verify_lines[:2]}")
    
    prompt = """
Проанализируй каждый лог ошибки и извлеки САМУЮ ВАЖНУЮ информацию об ошибке.

🔥 КРИТИЧЕСКИ ВАЖНО - ОБЯЗАТЕЛЬНО найди и включи в ответ:
1. Точное сообщение об ошибке (например: "Function 'DateTime2.Format' type mismatch", "Mkql memory limit exceeded", "VERIFY failed", "Unknown node in OLAP comparison compiler")
2. Конкретные технические детали ошибки (коды ошибок, лимиты памяти, типы несоответствий)
3. Контекст где произошла ошибка (файл, функция, строка если есть)

🚨 СПЕЦИАЛЬНОЕ ВНИМАНИЕ К VERIFY ОШИБКАМ:
Если в логе есть строки с "VERIFY failed", это КРИТИЧЕСКИ ВАЖНАЯ информация об ошибках памяти.
ВСЕГДА включай полную VERIFY информацию:
- "VERIFY failed (Z): Allocated: [числа], Freed: [числа], Peak: [числа]"
- "VerifyDebug(): requirement GetUsage() == 0 failed"
- Любые другие детали VERIFY ошибок

ОБЯЗАТЕЛЬНО ВКЛЮЧАЙ:
- 🔥 Ошибки верификации "VERIFY failed" с ПОЛНЫМИ числовыми деталями (Allocated, Freed, Peak)
- 🔥 Требования верификации "requirement GetUsage() == 0 failed"
- Сообщения об ошибках типа "Function ... type mismatch"
- Ошибки памяти "Mkql memory limit exceeded" с деталями
- Ошибки компиляции "Unknown node in OLAP comparison compiler"
- Ошибки запуска "KiKiMR start failed", "Daemon failed"
- Ошибки процессов "Cannot kill a stopped process"
- Assertion ошибки с конкретными условиями
- Timeout ошибки с временными лимитами

НЕ ВКЛЮЧАЙ:
- Общие фразы типа "Тест завершился ошибкой"
- Пути к файлам и технические детали без смысла
- Повторяющиеся строки
- Информационные сообщения

ПРИМЕРЫ ХОРОШИХ ОТВЕТОВ:
- "VERIFY failed (Z): Allocated: 41981872, Freed: 41975776, Peak: 6726904. VerifyDebug(): requirement GetUsage() == 0 failed. Daemon failed with message: Unexpectedly finished before stop."
- "Function 'DateTime2.Format' type mismatch: expected Type (Callable) with DateTime2.TM64, actual Type (Callable) with DateTime2.TM"
- "Mkql memory limit exceeded: allocated 264306688 bytes by task 1, tx total memory allocations: 408MiB"
- "KiKiMR start failed: bootstrap controller timeout в методе __wait_for_bs_controller_to_start"

🔥 ВАЖНО: Если видишь "VERIFY failed" - это ПРИОРИТЕТ №1! Включи ВСЕ детали VERIFY.

ФОРМАТ ОТВЕТА (строго JSON):
{
  "log_0": "конкретная ошибка с техническими деталями",
  "log_1": "конкретная ошибка с техническими деталями",
  ...
}

Максимум 800 символов на лог. Отвечай ТОЛЬКО JSON, без комментариев.
"""
    
    try:
        logging.debug("🤖 Отправляем запрос в AI...")
        response = call_single_ai_request(prompt, batch_data)
        
        if response:
            logging.debug(f"🤖 AI ответил, длина ответа: {len(response)} символов")
            logging.debug(f"🤖 AI ответ начинается с: {response[:100]}...")
            
            # Пытаемся распарсить JSON
            cleaned_response = response.strip()
            if cleaned_response.startswith('```json'):
                cleaned_response = cleaned_response[7:-3]
            elif cleaned_response.startswith('```'):
                cleaned_response = cleaned_response[3:-3]
            
            ai_result = json.loads(cleaned_response)
            
            # ДЕТАЛЬНОЕ ЛОГИРОВАНИЕ результата AI
            logging.debug(f"🤖 AI результат успешно распарсен, ключей: {len(ai_result)}")
            for key, result in ai_result.items():
                has_verify_result = 'VERIFY' in result if result else False
                logging.debug(f"  📤 {key}: длина={len(result) if result else 0}, VERIFY={'✅' if has_verify_result else '❌'}")
                
                if has_verify_result:
                    logging.info(f"✅ VERIFY DEBUGGING - AI СОХРАНИЛ VERIFY информацию в {key}:")
                    logging.info(f"  📋 Результат: {result[:200]}...")
                elif any('VERIFY failed' in str(data.get('original_log', '')) or 'VERIFY failed' in str(data.get('basic_processing', '')) 
                        for data in batch_data.values() if isinstance(data, dict)):
                    logging.warning(f"❌ VERIFY DEBUGGING - AI НЕ СОХРАНИЛ VERIFY информацию в {key}:")
                    logging.warning(f"  📋 Результат без VERIFY: {result[:200]}...")
            
            return ai_result
        else:
            logging.warning("🤖 AI не вернул ответ")
            return None
            
    except json.JSONDecodeError as e:
        logging.warning(f"🤖 AI вернул невалидный JSON: {e}")
        logging.warning(f"🤖 Ответ AI: {response[:500]}...")
        return None
    except Exception as e:
        logging.warning(f"🤖 AI batch processing failed: {e}")
        return None


def smart_error_extraction_with_cache(test_data):
    """Умное извлечение ошибок с кешированием паттернов - теперь обрабатывает И mute И failure тесты"""
    logging.debug("=== УМНОЕ ИЗВЛЕЧЕНИЕ ОШИБОК С КЕШИРОВАНИЕМ ===")
    
    # РАСШИРЯЕМ ОБРАБОТКУ: теперь включаем И mute И failure тесты
    target_records = [r for r in test_data if r.get('status') in ['mute', 'failure'] and r.get('log')]
    if not target_records:
        logging.debug("Нет записей mute/failure с логами для обработки")
        return test_data
    
    mute_count = len([r for r in target_records if r.get('status') == 'mute'])
    failure_count = len([r for r in target_records if r.get('status') == 'failure'])
    logging.debug(f"Обрабатываем {len(target_records)} записей: {mute_count} mute + {failure_count} failure")
    
    # Статистика
    cache_hits = 0
    basic_processed = 0
    ai_processed = 0
    failed_downloads = 0
    xml_sufficient = 0  # Новая метрика для failure тестов с достаточным XML
    
    # Обрабатываем каждый лог по отдельности
    for idx, record in enumerate(target_records):
        log_url = record.get('log')
        test_name = f"{record.get('suite_folder', '')}/{record.get('test_name', '')}"
        status = record.get('status')
        
        logging.debug(f"Обрабатываем лог {idx+1}/{len(target_records)} ({status}): {test_name}")
        
        # ДЛЯ FAILURE ТЕСТОВ: сначала проверяем качество XML описания
        if status == 'failure':
            xml_description = record.get('status_description', '')
            if is_xml_description_sufficient(xml_description):
                logging.debug(f"  XML описание достаточно информативно для failure теста")
                xml_sufficient += 1
                continue  # Пропускаем AI обработку - XML описание достаточно
            else:
                logging.debug(f"  XML описание недостаточно для failure теста: {xml_description[:100]}...")
        
        # Шаг 1: Скачиваем лог
        log_content = fetch_log_content(log_url)
        if not log_content:
            logging.debug(f"  Не удалось скачать лог")
            failed_downloads += 1
            continue
        
        # Шаг 2: Проверяем кеш
        cached_error = error_cache.find_matching_pattern(log_content)
        
        if cached_error:
            # Найден в кеше
            record['status_description'] = cached_error
            cache_hits += 1
            logging.debug(f"  Cache HIT - используем кешированный результат")
        else:
            # Не найден в кеше - обрабатываем
            logging.debug(f"  Cache MISS - обрабатываем лог")
            
            # Шаг 3: Базовая обработка
            basic_result = extract_meaningful_error_info(log_content, max_length=1500, log_url=log_url)
            
            # ДЕТАЛЬНОЕ ЛОГИРОВАНИЕ для VERIFY ошибок
            has_verify_in_log = 'VERIFY' in log_content
            has_verify_in_basic = 'VERIFY' in basic_result
            if has_verify_in_log:
                logging.info(f"🔍 VERIFY DEBUGGING - Лог {test_name} ({status}):")
                logging.info(f"  📋 Исходный лог содержит VERIFY: ✅")
                logging.info(f"  📤 Базовая обработка содержит VERIFY: {'✅' if has_verify_in_basic else '❌'}")
                if not has_verify_in_basic:
                    logging.warning(f"  ⚠️  VERIFY информация потеряна в базовой обработке!")
                    logging.info(f"  📋 Базовая обработка: {basic_result[:200]}...")
            
            # Шаг 4: Определяем, нужен ли AI
            if needs_ai_processing(basic_result):
                # Нужна AI обработка
                logging.debug(f"  Требуется AI обработка")
                
                if has_verify_in_log:
                    logging.info(f"  🤖 VERIFY DEBUGGING - AI обработка для VERIFY ошибки ({status})")
                
                # Подготавливаем данные для AI
                ai_data = {
                    "log_0": {
                        'original_log': log_content[:8000],  # Ограничиваем размер
                        'basic_processing': basic_result
                    }
                }
                
                # ДЕТАЛЬНОЕ ЛОГИРОВАНИЕ данных для AI
                if has_verify_in_log:
                    has_verify_in_ai_original = 'VERIFY' in ai_data["log_0"]['original_log']
                    has_verify_in_ai_basic = 'VERIFY' in ai_data["log_0"]['basic_processing']
                    logging.info(f"  📤 AI данные - original_log содержит VERIFY: {'✅' if has_verify_in_ai_original else '❌'}")
                    logging.info(f"  📤 AI данные - basic_processing содержит VERIFY: {'✅' if has_verify_in_ai_basic else '❌'}")
                    
                    if not has_verify_in_ai_original:
                        logging.warning(f"  ⚠️  VERIFY информация потеряна при урезании лога до 8000 символов!")
                        # Ищем VERIFY в исходном логе
                        verify_lines = [line for line in log_content.split('\n') if 'VERIFY failed' in line]
                        if verify_lines:
                            logging.info(f"  📋 VERIFY строки в исходном логе: {verify_lines[:3]}")
                
                # AI запрос
                ai_results = process_error_batch_with_ai(ai_data)
                
                if ai_results and ai_results.get("log_0"):
                    # AI успешно обработал
                    final_result = ai_results["log_0"]
                    ai_processed += 1
                    logging.debug(f"  AI обработка успешна")
                    
                    # ДЕТАЛЬНОЕ ЛОГИРОВАНИЕ результата AI
                    if has_verify_in_log:
                        has_verify_in_ai_result = 'VERIFY' in final_result
                        logging.info(f"  📤 AI результат содержит VERIFY: {'✅' if has_verify_in_ai_result else '❌'}")
                        logging.info(f"  📋 AI результат: {final_result[:300]}...")
                        
                        if not has_verify_in_ai_result:
                            logging.warning(f"  ❌ AI НЕ СОХРАНИЛ VERIFY ИНФОРМАЦИЮ!")
                            logging.warning(f"  📋 Полный AI результат: {final_result}")
                            logging.warning(f"  🔍 Анализ проблемы:")
                            logging.warning(f"    - Исходный лог имел VERIFY: ✅")
                            logging.warning(f"    - Базовая обработка имела VERIFY: {'✅' if has_verify_in_basic else '❌'}")
                            logging.warning(f"    - AI original_log имел VERIFY: {'✅' if 'VERIFY' in ai_data['log_0']['original_log'] else '❌'}")
                            logging.warning(f"    - AI basic_processing имел VERIFY: {'✅' if 'VERIFY' in ai_data['log_0']['basic_processing'] else '❌'}")
                            logging.warning(f"    - AI результат имеет VERIFY: ❌")
                        else:
                            logging.info(f"  ✅ AI ПРАВИЛЬНО СОХРАНИЛ VERIFY ИНФОРМАЦИЮ")
                else:
                    # AI не сработал, используем базовую обработку
                    final_result = basic_result
                    basic_processed += 1
                    logging.debug(f"  AI не сработал, используем базовую обработку")
                    
                    if has_verify_in_log:
                        logging.warning(f"  ❌ AI НЕ СРАБОТАЛ для VERIFY ошибки!")
                        logging.info(f"  📋 Используем базовую обработку: {final_result[:200]}...")
                        logging.info(f"  📤 Базовая обработка содержит VERIFY: {'✅' if 'VERIFY failed' in final_result else '❌'}")
            else:
                # Базовой обработки достаточно
                final_result = basic_result
                basic_processed += 1
                logging.debug(f"  Базовая обработка достаточна")
                
                if has_verify_in_log:
                    logging.info(f"  📋 VERIFY DEBUGGING - AI не нужен, используем базовую обработку")
                    logging.info(f"  📤 Финальный результат содержит VERIFY: {'✅' if 'VERIFY failed' in final_result else '❌'}")
                    if 'VERIFY failed' not in final_result:
                        logging.warning(f"  ⚠️  VERIFY информация потеряна даже в базовой обработке!")
                        logging.warning(f"  📋 Базовая обработка: {final_result}")
            
            # Шаг 5: Устанавливаем результат и добавляем в кеш
            record['status_description'] = final_result
            error_cache.add_pattern(log_content, final_result)
            logging.debug(f"  Добавлено в кеш")
            
            # ФИНАЛЬНАЯ ПРОВЕРКА для VERIFY ошибок
            if has_verify_in_log:
                has_verify_in_final = 'VERIFY' in final_result
                logging.info(f"  🎯 ИТОГОВЫЙ РЕЗУЛЬТАТ для VERIFY: {'✅ Сохранено' if has_verify_in_final else '❌ Потеряно'}")
                if not has_verify_in_final:
                    logging.error(f"  💥 КРИТИЧЕСКАЯ ПРОБЛЕМА: VERIFY информация потеряна в финальном результате!")
                    logging.error(f"  📋 Финальный результат: {final_result}")
                    logging.error(f"  🔍 Полная цепочка обработки:")
                    logging.error(f"    1. Исходный лог содержал VERIFY: ✅")
                    logging.error(f"    2. Базовая обработка содержала VERIFY: {'✅' if has_verify_in_basic else '❌'}")
                    logging.error(f"    3. AI {'был вызван' if needs_ai_processing(basic_result) else 'НЕ был вызван'}")
                    logging.error(f"    4. Финальный результат содержит VERIFY: ❌")
                else:
                    logging.info(f"  ✅ VERIFY информация успешно сохранена через всю цепочку обработки")
    
    # Сохраняем кеш
    error_cache.save_cache()
    
    # УЛУЧШЕННАЯ СТАТИСТИКА
    total_processed = len(target_records) - failed_downloads
    logging.info(f"=== СТАТИСТИКА ОБРАБОТКИ ОШИБОК ===")
    logging.info(f"Всего записей: {len(target_records)} (mute: {mute_count}, failure: {failure_count})")
    logging.info(f"Не удалось скачать: {failed_downloads}")
    logging.info(f"XML описание достаточно (failure): {xml_sufficient}")
    logging.info(f"Успешно обработано: {total_processed}")
    logging.info(f"Кеш попадания: {cache_hits} ({cache_hits/total_processed*100:.1f}% если total_processed > 0)")
    logging.info(f"Базовая обработка: {basic_processed}")
    logging.info(f"AI обработка: {ai_processed}")
    logging.info(f"Новых паттернов: {error_cache.new_patterns_count}")
    logging.info(f"Всего паттернов в кеше: {len(error_cache.patterns)}")
    
    # КРИТИЧЕСКОЕ ОТКРЫТИЕ - логируем информацию о расширении обработки
    logging.info(f"🔥 КРИТИЧЕСКОЕ УЛУЧШЕНИЕ: Теперь обрабатываем И mute И failure тесты!")
    logging.info(f"   Ранее: только mute тесты ({mute_count}) получали AI анализ")
    logging.info(f"   Теперь: mute + failure тесты ({len(target_records)}) получают AI анализ")
    logging.info(f"   Это должно ЗНАЧИТЕЛЬНО улучшить категоризацию daemon crashes и других critical ошибок!")
    
    return test_data


def rebuild_cache_with_improved_normalization():
    """Очищает и пересоздает кеш с улучшенной нормализацией"""
    logging.info("=== ПЕРЕСОЗДАНИЕ КЕША С УЛУЧШЕННОЙ НОРМАЛИЗАЦИЕЙ ===")
    
    # Анализируем текущий кеш
    if len(error_cache.patterns) > 0:
        logging.info("Анализ текущего кеша:")
        error_cache.analyze_cache_effectiveness()
        
        # Очищаем кеш
        logging.info("Очищаем кеш для пересоздания...")
        error_cache.clear_cache()
    else:
        logging.info("Кеш пуст, будет создан новый")
    
    return True


def generate_version_report(version, version_data, ai_ready_data, output_dir):
    """Генерирует улучшенный отчет для конкретной версии с анализом совместимости"""
    logging.debug(f"=== Generating enhanced report for version: {version} ===")
    
    # ГЕНЕРИРУЕМ ФИНАЛЬНЫЙ ОТЧЕТ С УЛУЧШЕННЫМ ФОРМАТОМ
    logging.debug("Step 1: Generating enhanced final report...")
    
    # Используем улучшенную функцию с анализом совместимости
    logging.debug("Using enhanced report with compatibility analysis")
    report = generate_enhanced_version_report_with_compatibility(version_data, ai_ready_data, version)
    
    # СОХРАНЯЕМ ОТЧЕТ
    report_filename = f"compatibility_report_{version.replace('_', '-').replace('-', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
    report_path = os.path.join(output_dir, report_filename)
    
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(report)
    
    logging.debug(f"=== Enhanced version report saved: {report_path} ===")
    return report_path


def generate_enhanced_version_report_with_compatibility(version_data, ai_ready_data, version):
    """Генерирует улучшенный отчет для конкретной версии с анализом совместимости"""
    stats = ai_ready_data['summary_stats']['by_version'].get(version, {})
    
    # Анализируем тесты по категориям совместимости
    single_version_tests = []
    compatibility_tests = []
    
    for test_type, type_data in version_data.items():
        for test in type_data.get('all_tests', []):
            if test.get('recent_runs'):
                first_run = test['recent_runs'][0]
                if first_run.get('compatibility_category') == 'single_version':
                    single_version_tests.append(test)
                else:
                    compatibility_tests.append(test)
    
    # Определяем общий статус и тренд
    success_rate = (stats.get('passed', 0) / stats.get('total', 1) * 100) if stats.get('total', 0) else 0
    
    if success_rate >= 90:
        status_emoji = "🟢"
        status_text = "Good"
    elif success_rate >= 70:
        status_emoji = "🟡"
        status_text = "Warning"
    else:
        status_emoji = "🔴"
        status_text = "Critical"
    
    # Для тренда пока используем статический символ (требует исторических данных)
    trend_emoji = "➡️"  # TODO: реализовать сравнение с предыдущими запусками
    
    report = f"""# 🔍 YDB Compatibility Report for {version}

## Executive Summary
**Overall Status:** {status_emoji} {status_text} | **Success Rate:** {success_rate:.1f}% | **Trend:** {trend_emoji}

### Key Metrics
| Total Tests | Passed | Failed | Muted | New Issues | Single Version | Cross-Version |
|-------------|--------|--------|-------|------------|----------------|---------------|
| {stats.get('total', 0)} | {stats.get('passed', 0)} | {stats.get('failure', 0)} | {stats.get('mute', 0)} | 0 | {len(single_version_tests)} | {len(compatibility_tests)} |

### Test Categories Breakdown
- **🔧 Single Version Tests**: {len(single_version_tests)} тестов (тестирование только версии {version})
- **🔄 Cross-Version Compatibility**: {len(compatibility_tests)} тестов (совместимость с другими версиями)

---

## 🚨 Critical Issues (New & High Priority)
"""
    
    # Новые проблемы - высший приоритет
    new_failures = []
    for test_type, type_data in version_data.items():
        new_failures.extend(type_data.get('new_failures', []))
    
    if new_failures:
        report += f"### 🆕 New Failures ({len(new_failures)})\n"
        report += "| Test | Error | Test Type |\n|------|--------|----------|\n"
        for test in new_failures[:10]:  # Топ-10 новых проблем
            error = extract_error_for_display(test.get('error_description', ''), max_length=200)
            context = 'N/A'
            if test.get('recent_runs'):
                context = test['recent_runs'][0].get('test_context', 'N/A')
            report += f"| {test['name']} | {error} | {context} |\n"
        if len(new_failures) > 10:
            report += f"\n*... and {len(new_failures) - 10} more new failures*\n"
    else:
        report += "✅ **No new failures detected**\n"
    
    # Постоянно падающие тесты
    failed_tests = []
    for test_type, type_data in version_data.items():
        failed_tests.extend(type_data.get('failed_tests', []))
    
    if failed_tests:
        report += f"\n### ⚠️ Consistently Failing Tests ({len(failed_tests)})\n"
        
        # Сначала показываем группировку для обзора
        error_groups = group_tests_by_error(failed_tests)
        
        # Создаем маппинг тестов к номерам паттернов
        test_to_pattern = {}
        if len(error_groups) > 1:  # Если есть группы, показываем обзор
            report += "**Error Patterns Overview:**\n"
            report += "| Error Pattern | Tests Count | Representative Error |\n"
            report += "|---------------|-------------|----------------------|\n"
            
            # Сортируем группы по количеству тестов (убывание)
            sorted_groups = sorted(error_groups.items(), key=lambda x: len(x[1]['tests']), reverse=True)
            
            for i, (error_key, group_data) in enumerate(sorted_groups, 1):
                tests_count = len(group_data['tests'])
                error_desc = extract_error_for_display(group_data['representative_error'], max_length=200)
                report += f"| Pattern #{i} | {tests_count} | {error_desc} |\n"
                
                # Сохраняем маппинг тестов к номеру паттерна
                for test in group_data['tests']:
                    test_to_pattern[test['name']] = i
            
            report += "\n"
        
        # Затем всегда показываем детальную таблицу всех тестов
        report += "**Detailed Test Failures:**\n"
        report += "| Test | Failure Rate | Pattern | Error Pattern | Context |\n|------|--------------|---------|---------------|----------|\n"
        for test in failed_tests:
            error_pattern = format_error_pattern_for_table(test.get('error_description', ''), log_url=test.get('log_url', ''))
            context = 'N/A'
            if test.get('recent_runs'):
                context = test['recent_runs'][0].get('test_context', 'N/A')
            fail_rate = test.get('fail_rate', 0) * 100
            
            # Получаем номер паттерна для этого теста
            pattern_ref = f"#{test_to_pattern[test['name']]}" if test['name'] in test_to_pattern else "N/A"
            
            report += f"| {test['name']} | {fail_rate:.1f}% | {pattern_ref} | {error_pattern} | {context} |\n"
    
    report += "\n---\n\n## 🔄 Stability Analysis\n"
    
    # Нестабильные тесты
    flaky_tests = []
    for test_type, type_data in version_data.items():
        flaky_tests.extend(type_data.get('flaky_tests', []))
    
    if flaky_tests:
        report += f"### 📊 Flaky Tests ({len(flaky_tests)})\n"
        report += "*Tests showing inconsistent behavior - both passes and failures in recent runs*\n\n"
        report += "| Test | Pattern | Failure Rate | Most Common Error | Trend | Context |\n"
        report += "|------|---------|--------------|-------------------|-------|----------|\n"
        
        for test in flaky_tests:  # Показываем ВСЕ flaky тесты
            # Анализируем паттерн последних запусков
            recent_runs = test.get('recent_runs', [])[:10]  # Последние 10 запусков
            pattern = ""
            for run in recent_runs:
                status = run.get('status', 'U')
                if status == 'passed':
                    pattern += "P"
                elif status in ['failure', 'mute']:
                    pattern += "F"
                elif status == 'skipped':
                    pattern += "S"
                else:
                    pattern += "U"
            
            # Определяем тренд (последние 5 vs предыдущие 5)
            if len(recent_runs) >= 6:
                recent_5 = recent_runs[:5]
                prev_5 = recent_runs[5:10] if len(recent_runs) >= 10 else recent_runs[5:]
                
                recent_fail_rate = len([r for r in recent_5 if r.get('status') in ['failure', 'mute']]) / len(recent_5)
                prev_fail_rate = len([r for r in prev_5 if r.get('status') in ['failure', 'mute']]) / len(prev_5) if prev_5 else 0
                
                if recent_fail_rate > prev_fail_rate + 0.2:
                    trend = "📈 Worsening"
                elif recent_fail_rate < prev_fail_rate - 0.2:
                    trend = "📉 Improving"
                else:
                    trend = "➡️ Stable"
            else:
                trend = "❓ Insufficient data"
            
            # Находим самую частую ошибку
            error_descriptions = []
            for run in recent_runs:
                if run.get('status') in ['failure', 'mute'] and run.get('status_description'):
                    error_descriptions.append(run.get('status_description'))
            
            most_common_error = "No error info"
            if error_descriptions:
                # Группируем похожие ошибки
                error_groups = {}
                for error_desc in error_descriptions:
                    error_key = normalize_error_for_grouping(error_desc)
                    if error_key not in error_groups:
                        error_groups[error_key] = {'count': 0, 'example': error_desc}
                    error_groups[error_key]['count'] += 1
                
                # Находим самую частую
                if error_groups:
                    most_frequent = max(error_groups.items(), key=lambda x: x[1]['count'])
                    most_common_error = extract_error_for_display(most_frequent[1]['example'], max_length=150)
                    if most_frequent[1]['count'] > 1:
                        most_common_error += f" ({most_frequent[1]['count']}x)"
            
            fail_rate = test.get('fail_rate', 0) * 100
            context = 'N/A'
            if test.get('recent_runs'):
                context = test['recent_runs'][0].get('test_context', 'N/A')
            
            report += f"| {test['name']} | `{pattern}` | {fail_rate:.1f}% | {most_common_error} | {trend} | {context} |\n"
        
        # Добавляем пояснение к паттерну
        report += "\n*Pattern Legend: P=Pass, F=Fail, S=Skip, U=Unknown (most recent first)*\n"
    else:
        report += "✅ **No flaky tests detected - all tests show consistent behavior**\n"
    
    report += "\n---\n\n## 📈 Test Coverage Breakdown\n"
    report += "| Test Type | Total | Passed | Failed | Muted | Flaky | Success Rate |\n"
    report += "|-----------|-------|--------|--------|-------|-------|---------------|\n"
    
    for test_type, type_data in version_data.items():
        all_tests = type_data.get('all_tests', [])
        total = len(all_tests)
        
        if total == 0:
            continue
            
        passed_count = len([t for t in all_tests if t.get('latest_status') == 'passed'])
        failed = len(type_data.get('failed_tests', []))
        muted_count = len([t for t in all_tests if t.get('latest_status') == 'mute'])
        flaky = len(type_data.get('flaky_tests', []))
        
        success_rate_type = (passed_count / total * 100) if total else 0
        report += f"| {test_type} | {total} | {passed_count} | {failed} | {muted_count} | {flaky} | {success_rate_type:.1f}% |\n"
    
    # Детальный анализ по категориям совместимости
    report += f"\n---\n\n## 📊 Detailed Analysis by Test Categories\n\n"
    
    # Анализ тестов одной версии
    if single_version_tests:
        single_failed = [t for t in single_version_tests if t['latest_status'] in ['failure', 'mute']]
        single_success_rate = ((len(single_version_tests) - len(single_failed)) / len(single_version_tests) * 100) if single_version_tests else 0
        
        report += f"### 🔧 Single Version Tests ({version} only)\n"
        report += f"**Success Rate:** {single_success_rate:.1f}% | **Total:** {len(single_version_tests)} | **Failed:** {len(single_failed)}\n\n"
        
        if single_failed:
            report += "**Failed Single Version Tests:**\n"
            report += "| Test | Status | Error Pattern |\n|------|--------|---------------|\n"
            for test in single_failed:  # Показываем ВСЕ failed single version тесты
                error_pattern = format_error_pattern_for_table(test.get('error_description', ''), log_url=test.get('log_url', ''))
                report += f"| {test['name']} | {test['latest_status']} | {error_pattern} |\n"
    
    # Анализ тестов совместимости
    if compatibility_tests:
        compat_failed = [t for t in compatibility_tests if t['latest_status'] in ['failure', 'mute']]
        compat_success_rate = ((len(compatibility_tests) - len(compat_failed)) / len(compatibility_tests) * 100) if compatibility_tests else 0
        
        report += f"\n### 🔄 Cross-Version Compatibility Tests\n"
        report += f"**Success Rate:** {compat_success_rate:.1f}% | **Total:** {len(compatibility_tests)} | **Failed:** {len(compat_failed)}\n\n"
        
        # Группируем по парам версий
        version_pairs = {}
        for test in compatibility_tests:
            if test.get('recent_runs'):
                first_run = test['recent_runs'][0]
                pair_info = first_run.get('version_pair_info', {})
                pair_desc = pair_info.get('pair_description', 'unknown')
                
                if pair_desc not in version_pairs:
                    version_pairs[pair_desc] = {'total': 0, 'passed': 0, 'failed': 0, 'tests': []}
                
                version_pairs[pair_desc]['total'] += 1
                version_pairs[pair_desc]['tests'].append(test)
                
                if test['latest_status'] == 'passed':
                    version_pairs[pair_desc]['passed'] += 1
                elif test['latest_status'] in ['failure', 'mute']:
                    version_pairs[pair_desc]['failed'] += 1
        
        if version_pairs:
            report += "**Compatibility by Version Pairs:**\n"
            report += "| Version Pair | Total | Passed | Failed | Success Rate |\n"
            report += "|--------------|-------|--------|--------|---------------|\n"
            
            for pair_desc, pair_stats in sorted(version_pairs.items()):
                success_rate_pair = (pair_stats['passed'] / pair_stats['total'] * 100) if pair_stats['total'] else 0
                report += f"| {pair_desc} | {pair_stats['total']} | {pair_stats['passed']} | {pair_stats['failed']} | {success_rate_pair:.1f}% |\n"
        
        if compat_failed:
            report += f"\n**Failed Cross-Version Tests:**\n"
            report += "| Test | Status | Version Pair | Error Pattern |\n|------|--------|--------------|---------------|\n"
            for test in compat_failed:  # Показываем ВСЕ failed cross-version тесты
                error_pattern = format_error_pattern_for_table(test.get('error_description', ''), log_url=test.get('log_url', ''))
                context = 'N/A'
                if test.get('recent_runs'):
                    context = test['recent_runs'][0].get('test_context', 'N/A')
                report += f"| {test['name']} | {test['latest_status']} | {context} | {error_pattern} |\n"
    
    # AI анализ ошибок (улучшенный)
    if failed_tests or flaky_tests:
        report += f"\n---\n\n## 🤖 AI Error Analysis\n\n"
        
        # Улучшенная группировка ошибок с извлечением содержательной информации
        error_clusters = {}
        all_problem_tests = failed_tests + flaky_tests
        
        for test in all_problem_tests[:20]:  # Анализируем топ-20 проблемных тестов
            error_desc = test.get('error_description', '')
            if not error_desc:
                continue
            
            # Извлекаем содержательную часть ошибки для анализа
            meaningful_error = extract_meaningful_error_info(error_desc, max_length=500)
            error_desc_lower = meaningful_error.lower()
            
            # Определяем тип ошибки на основе содержательной информации
            error_type = 'Generic Execution Failure'
            key_error = 'Execution failed with exit code: 1'
            
            # DAEMON CRASHES (высший приоритет - критичные системные ошибки)
            if any(marker in error_desc_lower for marker in [
                'severaldaemonerrors:', 'daemon failed with message:', 'unexpectedly finished before stop',
                'process exit_code = -', 'exit_code = -11', 'exit_code = -6'
            ]):
                error_type = 'Daemon Crash and System Failures'
                # Извлекаем детали crash'а
                crash_details = []
                if 'exit_code = -11' in error_desc_lower:
                    crash_details.append('SIGSEGV (segmentation fault)')
                if 'exit_code = -6' in error_desc_lower:
                    crash_details.append('SIGABRT (abort signal)')
                if 'unexpectedly finished before stop' in error_desc_lower:
                    crash_details.append('unexpected termination')
                if 'not freed 0x' in error_desc_lower:
                    crash_details.append('memory leaks detected')
                
                if crash_details:
                    key_error = f"Daemon crash: {', '.join(crash_details)}"
                else:
                    key_error = 'Multiple daemon failures with unexpected termination'
                    
            # MEMORY VERIFICATION ошибки (высокий приоритет)
            elif 'verify' in error_desc_lower:
                error_type = 'Memory Verification Errors (VERIFY)'
                # Извлекаем конкретные детали VERIFY
                verify_match = re.search(r'verify failed.*?allocated:.*?\d+.*?freed:.*?\d+.*?peak:.*?\d+', meaningful_error, re.IGNORECASE | re.DOTALL)
                if verify_match:
                    key_error = verify_match.group(0)[:100] + "..."
                else:
                    key_error = 'VERIFY failed: Memory verification error with allocation details'
                    
            # Memory limit ошибки
            elif 'mkql memory limit exceeded' in error_desc_lower:
                error_type = 'Mkql Memory Limit Exceeded'
                # Извлекаем конкретные числа лимитов
                memory_match = re.search(r'mkql memory limit exceeded.*?allocated.*?\d+.*?bytes', meaningful_error, re.IGNORECASE)
                if memory_match:
                    key_error = memory_match.group(0)
                else:
                    key_error = 'Mkql memory limit exceeded: Memory allocation limit reached'
                    
            # Type mismatch ошибки
            elif 'type mismatch' in error_desc_lower or 'function.*type mismatch' in error_desc_lower:
                error_type = 'Function Type Mismatch Errors'
                type_match = re.search(r'function.*?type mismatch.*?expected.*?actual.*?', meaningful_error, re.IGNORECASE | re.DOTALL)
                if type_match:
                    key_error = type_match.group(0)[:150] + "..."
                else:
                    key_error = 'Function type mismatch: Expected vs actual parameter types differ'
                    
            # Database resolution ошибки
            elif 'database resolve failed' in error_desc_lower:
                error_type = 'Database Resolution Failure'
                key_error = 'Database resolve failed with no certain result'
                
            # Configuration ошибки
            elif 'unknown field' in error_desc_lower:
                error_type = 'Configuration and Startup Errors'
                config_match = re.search(r'unknown field.*?"[^"]*"', meaningful_error, re.IGNORECASE)
                if config_match:
                    key_error = config_match.group(0)
                else:
                    key_error = 'Configuration error: Unknown field in config'
                    
            # Timeout ошибки
            elif 'timeout' in error_desc_lower:
                error_type = 'Request/Process Timeout'
                timeout_match = re.search(r'timeout.*?exceeded|bootstrap.*?timeout', meaningful_error, re.IGNORECASE)
                if timeout_match:
                    key_error = timeout_match.group(0)
                else:
                    key_error = 'Timeout exceeded during operation'
                    
            # Daemon startup ошибки
            elif 'daemon failed' in error_desc_lower or 'kikimr start failed' in error_desc_lower:
                error_type = 'Daemon Startup Errors'
                daemon_match = re.search(r'daemon failed.*?|kikimr start failed.*?', meaningful_error, re.IGNORECASE)
                if daemon_match:
                    key_error = daemon_match.group(0)[:100] + "..."
                else:
                    key_error = 'Daemon startup failure: Process failed to start properly'
                    
            # OLAP compiler ошибки
            elif 'unknown node in olap' in error_desc_lower:
                error_type = 'OLAP Compilation Errors'
                key_error = 'Unknown node in OLAP comparison compiler'
                
            # Process management ошибки
            elif 'cannot kill' in error_desc_lower:
                error_type = 'Process Management Errors'
                key_error = 'Cannot kill a stopped process: Process state inconsistency'
                
            # Assertion ошибки
            elif 'assertion' in error_desc_lower or 'requirement.*failed' in error_desc_lower:
                error_type = 'Assertion and Requirement Failures'
                assertion_match = re.search(r'assertion.*?failed|requirement.*?failed', meaningful_error, re.IGNORECASE)
                if assertion_match:
                    key_error = assertion_match.group(0)
                else:
                    key_error = 'Assertion or requirement check failed'
            
            # Добавляем в кластер
            if error_type not in error_clusters:
                error_clusters[error_type] = {'key_error': key_error, 'tests': []}
            error_clusters[error_type]['tests'].append(test['name'])
        
        # Выводим кластеры (сортируем по важности)
        cluster_priority = {
            'Memory Verification Errors (VERIFY)': 1,
            'Mkql Memory Limit Exceeded': 2,
            'Function Type Mismatch Errors': 3,
            'OLAP Compilation Errors': 4,
            'Database Resolution Failure': 5,
            'Daemon Startup Errors': 6,
            'Configuration and Startup Errors': 7,
            'Assertion and Requirement Failures': 8,
            'Process Management Errors': 9,
            'Request/Process Timeout': 10,
            'Generic Execution Failure': 99
        }
        
        sorted_clusters = sorted(error_clusters.items(), 
                               key=lambda x: (cluster_priority.get(x[0], 50), -len(x[1]['tests'])))
        
        for i, (error_type, cluster_data) in enumerate(sorted_clusters, 1):
            report += f"## Cluster {i}: {error_type}\n"
            report += f"**Key Error:** `{cluster_data['key_error']}`\n"
            report += f"**Affected Tests:**\n"
            for test_name in cluster_data['tests']:  # Показываем ВСЕ тесты в кластере
                report += f"- {test_name}\n"
            report += "\n"
    
    # Рекомендации
    report += f"---\n\n## 🎯 Recommended Actions\n\n"
    
    if failed_tests:
        report += f"- ⚠️ **HIGH**: Fix {len(failed_tests)} consistently failing tests\n"
    if flaky_tests:
        report += f"- 🔄 **MEDIUM**: Stabilize {len(flaky_tests)} flaky tests\n"
    if new_failures:
        report += f"- 🆕 **HIGH**: Investigate {len(new_failures)} new failures\n"
    
    if not failed_tests and not flaky_tests and not new_failures:
        report += "- ✅ **All good!** No critical action items identified\n"
    
    report += f"""
---
## 📊 Summary Stats
- **Version:** {version}
- **Total Test Scenarios:** {stats.get('total', 0)}  
- **Overall Success Rate:** {success_rate:.1f}%
- **Critical Issues:** {len(new_failures)} new + {len(failed_tests)} persistent
- **Stability Issues:** {len(flaky_tests)} flaky tests
- **Single Version Tests:** {len(single_version_tests)} (testing {version} only)
- **Cross-Version Tests:** {len(compatibility_tests)} (compatibility with other versions)

---
*Report generated automatically for {version} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*
"""
    
    return report


def generate_version_mock_report(version_data, error_clustering_result=None, ai_report=None):
    """Генерирует mock-отчет для конкретной версии"""
    version = version_data['version']
    stats = version_data['summary_stats']
    
    report = f"""# Отчет по совместимости для версии {version}

## Статистика тестов

- Всего тестов: {stats['total_tests']}
- Passed: {stats['passed']}
- Failure: {stats['failure']}
- Mute: {stats['mute']}
- Успешность: {(stats['passed'] / stats['total_tests'] * 100) if stats['total_tests'] else 0:.1f}%

## Разбивка по типам тестов

"""
    
    # Таблица по типам тестов
    report += "| Тип теста | Всего | Упавшие | Нестабильные | Новые падения | Всегда skipped |\n"
    report += "|-----------|-------|---------|--------------|---------------|----------------|\n"
    
    for test_type, type_data in version_data['by_type'].items():
        total = len(type_data.get('all_tests', []))
        failed = len(type_data.get('failed_tests', []))
        flaky = len(type_data.get('flaky_tests', []))
        new_failures = len(type_data.get('new_failures', []))
        skipped = len(type_data.get('always_skipped_tests', []))
        
        success_rate_type = (stats['passed'] / stats['total_tests'] * 100) if stats['total_tests'] else 0
        report += f"| {test_type} | {total} | {stats['passed']} | {failed} | {stats['mute']} | {flaky} | {success_rate_type:.1f}% |\n"
    
    # Детальный анализ по категориям совместимости
    report += f"\n---\n\n## 📊 Detailed Analysis by Test Categories\n\n"
    
    # Анализ тестов одной версии
    if type_data.get('all_tests'):
        single_failed = [t for t in type_data['all_tests'] if t['latest_status'] in ['failure', 'mute']]
        single_success_rate = ((len(type_data['all_tests']) - len(single_failed)) / len(type_data['all_tests']) * 100) if type_data['all_tests'] else 0
        
        report += f"### 🔧 Single Version Tests ({version} only)\n"
        report += f"**Success Rate:** {single_success_rate:.1f}% | **Total:** {len(type_data['all_tests'])} | **Failed:** {len(single_failed)}\n\n"
        
        if single_failed:
            report += "**Failed Single Version Tests:**\n"
            report += "| Test | Status | Error Pattern |\n|------|--------|---------------|\n"
            for test in single_failed[:5]:
                error_pattern = extract_error_for_display(test.get('error_description', ''), 80)
                report += f"| {test['name']} | {test['latest_status']} | {error_pattern} |\n"
            if len(single_failed) > 5:
                report += f"\n*... and {len(single_failed) - 5} more failed single version tests*\n"
    
    # Тесты, не выполнявшиеся более суток
    if version_data['not_run_24h']:
        report += f"\n## Тесты, которые не выполнялись более суток ({len(version_data['not_run_24h'])})\n\n"
        report += "| Тест | Последний запуск |\n|------|------------------|\n"
        for test in sorted(version_data['not_run_24h'], key=lambda x: x['last_time'] or datetime(1970,1,1)):
            last_time_str = test['last_time'].strftime('%Y-%m-%d %H:%M:%S') if test['last_time'] else 'unknown'
            report += f"| {test['name']} | {last_time_str} |\n"
    
    # Нестабильные тесты за 24 часа
    if version_data['unstable_tests_24h']:
        report += f"\n## Нестабильные тесты за последние 24 часа ({len(version_data['unstable_tests_24h'])})\n\n"
        report += "| Тест | Статус | Частота падений | Ошибка |\n|------|--------|-----------------|--------|\n"
        for test in sorted(version_data['unstable_tests_24h'], key=lambda x: x['fail_rate'], reverse=True):
            err = extract_error_for_display(test['error_description'])
            report += f"| {test['name']} | {test['latest_status']} | {test['fail_rate']:.1%} | {err} |\n"
    
    # Кластеризация ошибок LLM
    if error_clustering_result:
        report += "\n## Кластеризация причин падений (LLM)\n\n"
        report += error_clustering_result + "\n"
    elif version_data['unstable_tests_24h']:
        # Базовая кластеризация если LLM недоступен
        report += "\n## Кластеризация причин падений (базовый анализ)\n\n"
        error_types = {}
        for test in version_data['unstable_tests_24h'][:10]:
            error = test.get('error_description', '')
            if error:
                if 'muted' in error.lower():
                    error_types.setdefault('Автоматически заглушенные тесты', []).append(test['name'])
                elif 'timeout' in error.lower():
                    error_types.setdefault('Проблемы с таймаутами', []).append(test['name'])
                else:
                    error_types.setdefault('Другие ошибки', []).append(test['name'])
        
        for i, (error_type, tests) in enumerate(error_types.items(), 1):
            report += f"### Кластер {i}: {error_type}\n"
            report += f"**Затронутые тесты:**\n"
            for test in tests[:5]:
                report += f"- {test}\n"
            if len(tests) > 5:
                report += f"- ... и еще {len(tests) - 5} тестов\n"
            report += "\n"
    
    # Детальный отчет от LLM
    if ai_report:
        report += "\n## Детальный анализ от LLM\n\n"
        report += ai_report + "\n"
    
    report += f"""
---
*Отчет сгенерирован автоматически для версии {version} в {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*
"""
    
    return report


def call_single_ai_request(prompt, data):
    """Делает один запрос к AI API с данными, используя умное сжатие при необходимости"""
    if not API_KEY:
        logging.warning("API_KEY not found, skipping AI request")
        return None
    
    headers = {
        'Content-Type': 'application/json',
        'x-api-key': API_KEY,
        'anthropic-version': '2023-06-01'
    }
    
    # Сначала пробуем без сжатия
    estimated_tokens = estimate_token_count(data) + len(prompt) // 3
    logging.debug(f"Initial data size: {estimated_tokens} tokens")
    
    if estimated_tokens > 180000:
        logging.debug("Data too large, applying smart compression...")
        processed_data = smart_compress_data_for_ai(data, target_token_limit=180000)
        new_estimated_tokens = estimate_token_count(processed_data) + len(prompt) // 3
        logging.debug(f"After smart compression: {new_estimated_tokens} tokens")
        
        # Если все еще слишком большой, применяем дополнительные меры
        if new_estimated_tokens > 180000:
            logging.warning("Still too large after smart compression, applying emergency reduction...")
            
            # Экстренное сокращение - берем только самые важные тесты
            if isinstance(processed_data, dict) and 'by_version' in processed_data:
                for version_data in processed_data['by_version'].values():
                    for type_data in version_data.values():
                        for category, tests in type_data.items():
                            if isinstance(tests, list):
                                # Сортируем по важности и берем топ-30
                                if category == 'all_tests':
                                    # Приоритет: mute/failure > остальные
                                    important_tests = [t for t in tests if t.get('latest_status') in ['mute', 'failure']]
                                    other_tests = [t for t in tests if t.get('latest_status') not in ['mute', 'failure']]
                                    
                                    # Берем топ-20 важных + топ-10 остальных
                                    type_data[category] = important_tests[:20] + other_tests[:10]
                                else:
                                    # Для других категорий берем топ-15
                                    type_data[category] = tests[:15]
            
            final_estimated_tokens = estimate_token_count(processed_data) + len(prompt) // 3
            logging.debug(f"After emergency reduction: {final_estimated_tokens} tokens")
    else:
        processed_data = data
    
    # Подготавливаем контент
    data_json = json.dumps(processed_data, ensure_ascii=False, indent=2, default=json_default)
    content = f"{prompt}\n\nДанные для анализа:\n{data_json}"
    
    final_size = len(content)
    final_tokens = final_size // 3
    
    logging.debug(f"Final request size: {final_size} characters ({final_tokens} tokens)")
    
    payload = {
       # 'model': 'internal-soy-yagpt',
        'model': 'claude-3-7-sonnet-20250219',
        'max_tokens': 8192,
        'messages': [
            {
                'role': 'user',
                'content': content
            }
        ]
    }
    
    try:
        response = requests.post(
            f"{ANTHROPIC_API_URL}/v1/messages",
            headers=headers,
            json=payload,
            timeout=120,
            verify=False
        )
        
        logging.debug(f"AI API response status: {response.status_code}")
        
        if response.status_code == 200:
            response_data = response.json()
            if 'content' in response_data and len(response_data['content']) > 0:
                return response_data['content'][0]['text']
            else:
                logging.debug("AI API returned empty content")
                return None
        elif response.status_code == 400:
            # Если все еще 400 ошибка, создаем минимальный набор данных
            logging.warning(f"AI API 400 Error: {response.text}")
            if "too long" in response.text.lower():
                logging.warning("Creating minimal dataset for analysis...")
                
                # Создаем минимальный набор с только ключевой информацией
                minimal_data = {
                    'summary': f"Анализ compatibility тестов",
                    'key_issues': []
                }
                
                # Собираем только самые критичные ошибки
                if isinstance(data, dict) and 'by_version' in data:
                    issue_count = 0
                    for version_data in data['by_version'].values():
                        for type_data in version_data.values():
                            for tests in type_data.values():
                                if isinstance(tests, list):
                                    for test in tests:
                                        if (test.get('latest_status') in ['mute', 'failure'] and 
                                            test.get('error_description') and 
                                            issue_count < 15):
                                            
                                            minimal_data['key_issues'].append({
                                                'test': test.get('name', '')[:80],
                                                'status': test.get('latest_status', ''),
                                                'error': extract_meaningful_error_info(
                                                    test.get('error_description', ''), 300
                                                )
                                            })
                                            issue_count += 1
                
                minimal_content = f"{prompt}\n\nМинимальные данные для анализа:\n{json.dumps(minimal_data, ensure_ascii=False, indent=2)}"
                payload['messages'][0]['content'] = minimal_content
                
                logging.debug(f"Minimal request size: {len(minimal_content)} characters")
                
                response = requests.post(
                    f"{ANTHROPIC_API_URL}/v1/messages",
                    headers=headers,
                    json=payload,
                    timeout=120,
                    verify=False
                )
                
                if response.status_code == 200:
                    response_data = response.json()
                    if 'content' in response_data and len(response_data['content']) > 0:
                        return response_data['content'][0]['text']
            
            return None
        else:
            logging.debug(f"AI API Error: {response.status_code} - {response.text}")
            return None
            
    except Exception as e:
        logging.debug(f"AI API Exception: {e}")
        return None


def extract_error_for_display(error_description, max_length=400):
    """Извлекает важную часть ошибки для отображения в отчете (использует ту же логику что и extract_meaningful_error_info)"""
    if not error_description:
        return 'No error description'
    
    lines = error_description.split('\n')
    error_start_index = -1
    
    # Ищем первую строку с ERROR (аналогично extract_meaningful_error_info)
    for i, line in enumerate(lines):
        if ' - ERROR - ' in line:
            error_start_index = i
            break
    
    # Если ERROR не найден, ищем другие критичные маркеры
    if error_start_index == -1:
        critical_markers = [' - EXCEPTION - ', ' - FATAL - ', ' - CRITICAL - ', 'Exception:', 'Error:', 
                          'unknown field', 'daemon failed', 'start failed', 'timeout', 'assertion']
        for marker in critical_markers:
            for i, line in enumerate(lines):
                if marker.lower() in line.lower():
                    error_start_index = i
                    break
            if error_start_index != -1:
                break
    
    # Берем полезную часть (от первой ошибки)
    if error_start_index != -1:
        useful_lines = lines[error_start_index:error_start_index+3]  # Максимум 3 строки от ошибки
        # Фильтруем DEBUG/INFO строки
        filtered_lines = []
        for line in useful_lines:
            line_stripped = line.strip()
            if not line_stripped:
                continue
            # Исключаем информационные строки
            if not any(pattern in line_stripped for pattern in [' - DEBUG - ', ' - INFO - ']):
                filtered_lines.append(line_stripped)
        
        result = ' | '.join(filtered_lines) if filtered_lines else 'Error details filtered'
    else:
        # Если критичных маркеров не найдено, берем первые непустые строки
        first_lines = [line.strip() for line in lines if line.strip()][:2]
        result = ' | '.join(first_lines) if first_lines else 'No clear error found'
    
    # Применяем базовую нормализацию для отображения
    result = re.sub(r'([a-zA-Z0-9/_.-]*)build_root/[a-zA-Z0-9/_.-]+', '[BUILD_PATH]', result)
    result = re.sub(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d+', '[TIMESTAMP]', result)
    result = re.sub(r':\d{4,5}\b', ':[PORT]', result)
    
    # Ограничиваем длину
    if len(result) > max_length:
        result = result[:max_length-3] + '...'
    
    return result


def determine_primary_version_for_grouping(test_type, compatibility_category, versions_list):
    """
    Определяет основную версию для группировки теста в отчете.
    
    Для тестов одной версии - возвращает эту версию.
    Для тестов совместимости - создаем отчеты для ВСЕХ участвующих версий.
    """
    if compatibility_category == "single_version":
        return [versions_list[0]] if versions_list else ["unknown"]
    
    # Для тестов совместимости возвращаем ВСЕ версии
    # Это позволит создать отчеты для каждой версии
    return versions_list if versions_list else ["unknown"]


def analyze_version_pairs(test_name, test_type, compatibility_category, versions_list):
    """
    Анализирует пары версий в тесте совместимости.
    Возвращает информацию о направлении и типе совместимости.
    """
    if compatibility_category == "single_version":
        return {
            'type': 'single_version',
            'primary_version': versions_list[0] if versions_list else 'unknown',
            'pair_description': f"single: {versions_list[0] if versions_list else 'unknown'}",
            'direction': 'none'
        }
    
    if len(versions_list) < 2:
        return {
            'type': 'unknown_compatibility',
            'primary_version': versions_list[0] if versions_list else 'unknown',
            'pair_description': 'unknown compatibility',
            'direction': 'unknown'
        }
    
    version_a, version_b = versions_list[0], versions_list[1]
    
    if test_type == 'mixed':
        return {
            'type': 'bidirectional',
            'primary_version': version_a,
            'secondary_version': version_b,
            'pair_description': f"compat: {version_a} ↔ {version_b}",
            'direction': 'bidirectional'
        }
    elif test_type in ['restart', 'rolling']:
        # Определяем направление из имени теста
        if '_to_' in test_name:
            # Формат: restart_A_to_B или rolling_A_to_B
            return {
                'type': 'migration',
                'primary_version': version_a,
                'secondary_version': version_b,
                'pair_description': f"migration: {version_a} → {version_b}",
                'direction': f"{version_a} → {version_b}"
            }
        else:
            return {
                'type': 'migration',
                'primary_version': version_a,
                'secondary_version': version_b,
                'pair_description': f"migration: {version_a} ↔ {version_b}",
                'direction': 'bidirectional'
            }
    
    return {
        'type': 'compatibility',
        'primary_version': version_a,
        'secondary_version': version_b,
        'pair_description': f"compat: {version_a} ↔ {version_b}",
        'direction': 'bidirectional'
    }


def generate_compatibility_report():
    setup_logging()
    logging.info("=== ЗАПУСК ГЕНЕРАЦИИ ОТЧЕТОВ ПО СОВМЕСТИМОСТИ ===")
    logging.info(f"Время запуска: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Проверяем переменные окружения
    logging.debug("Проверяем переменные окружения...")
    if "CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS" not in os.environ:
        logging.error("Ошибка: Переменная окружения CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS отсутствует")
        return 1
    else:
        logging.debug("Переменная CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS найдена")
        os.environ["YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"] = os.environ[
            "CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"
        ]

    # Подключение к БД
    logging.info(f"Подключаемся к БД: {DATABASE_ENDPOINT}")
    logging.info(f"Путь к БД: {DATABASE_PATH}")
    
    try:
        with ydb.Driver(
            endpoint=DATABASE_ENDPOINT,
            database=DATABASE_PATH,
            credentials=ydb.credentials_from_env_variables()
        ) as driver:
            logging.debug("Драйвер YDB создан, ожидаем подключения...")
            driver.wait(timeout=10, fail_fast=True)
            logging.info("✅ Успешно подключились к БД YDB")
            
            # ===== ЭТАП 1: ПОЛУЧИТЬ ДАННЫЕ ТЕСТОВ ИЗ БД =====
            logging.info("=== ЭТАП 1: ПОЛУЧЕНИЕ ДАННЫХ ИЗ БД ===")
            test_data = get_compatibility_tests_data(driver, days_back=1)
            
            if not test_data:
                logging.warning("Не найдено данных compatibility тестов в БД")
                return 0
            
            logging.info(f"Получено {len(test_data)} записей из БД")
            if DEBUG: save_json(test_data, 'analytics_debug_1_raw_data.json')
            
            # ===== ЭТАП 1.5: НОРМАЛИЗАЦИЯ STATUS_DESCRIPTION У FAILURE ТЕСТОВ =====
            logging.info("=== ЭТАП 1.5: НОРМАЛИЗАЦИЯ STATUS_DESCRIPTION ===")
            normalized_data = normalize_failure_status_description(test_data)
            
            logging.info(f"Нормализация завершена")
            if DEBUG: save_json(normalized_data, 'analytics_debug_1_5_normalized_data.json')
            
            # ===== ЭТАП 2: ЗАМЕНИТЬ STATUS_DESCRIPTION У MUTE РЕЗУЛЬТАТОВ ИЗ LOG =====
            logging.info("=== ЭТАП 2: ОБОГАЩЕНИЕ MUTE ЗАПИСЕЙ ЛОГАМИ ===")
            enriched_data = enrich_mute_records_with_logs(normalized_data)
            
            logging.info(f"Обогащение завершено")
            if DEBUG: save_json(enriched_data, 'analytics_debug_2_enriched_data.json')
            
            # ===== ЭТАП 3: ИСКЛЮЧИТЬ ТЕСТЫ БЕЗ STATUS_DESCRIPTION ИЗ LOG =====
            logging.info("=== ЭТАП 3: ФИЛЬТРАЦИЯ ЗАПИСЕЙ ===")
            filtered_data = filter_records_with_status_description(enriched_data)
            
            logging.info(f"После фильтрации осталось {len(filtered_data)} записей")
            if DEBUG: save_json(filtered_data, 'analytics_debug_3_filtered_data.json')
            
            # ===== ЭТАП 3.5: АНАЛИЗ РАСПАРСЕННЫХ ВЕРСИЙ =====
            logging.info("=== ЭТАП 3.5: АНАЛИЗ ВЕРСИЙ ===")
            version_stats = analyze_parsed_versions(filtered_data)
            if DEBUG: save_json(version_stats, 'analytics_debug_3_5_version_stats.json')
            
            # ===== ЭТАП 4: СГРУППИРОВАТЬ ПО ВЕРСИЯМ И ПРОВЕРКАМ =====
            logging.info("=== ЭТАП 4: ГРУППИРОВКА ПО ВЕРСИЯМ И ТИПАМ ===")
            grouped_data = group_by_versions_and_types(filtered_data)
            
            logging.info(f"Сгруппировано по {len(grouped_data)} версиям")
            if DEBUG: save_json(grouped_data, 'analytics_debug_4_grouped_data.json')
            
            # ===== ЭТАП 4.5: ФИЛЬТРАЦИЯ ВЕРСИЙ ДЛЯ ОТЧЕТОВ =====
            logging.info("=== ЭТАП 4.5: ФИЛЬТРАЦИЯ ВЕРСИЙ ===")
            filtered_grouped_data = filter_versions_for_reports(grouped_data)
            
            logging.info(f"После фильтрации версий осталось {len(filtered_grouped_data)} версий")
            if DEBUG: save_json(filtered_grouped_data, 'analytics_debug_4_5_filtered_versions.json')
            
            # ===== ЭТАП 5: СОБРАТЬ ГРУППЫ ДЛЯ ПЕРЕДАЧИ В AI =====
            logging.info("=== ЭТАП 5: ПОДГОТОВКА ДАННЫХ ДЛЯ AI ===")
            ai_ready_data = prepare_data_for_ai_analysis(filtered_grouped_data)
            
            logging.info(f"Подготовлены данные для AI анализа")
            if DEBUG: save_json(ai_ready_data, 'analytics_debug_5_ai_ready_data.json')
            
            # ===== ГЕНЕРАЦИЯ ОТЧЕТОВ =====
            logging.info("=== ГЕНЕРАЦИЯ ОТЧЕТОВ ===")
            
            # Создаем папку для отчетов
            reports_dir = f"{dir}/compatibility_reports_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            os.makedirs(reports_dir, exist_ok=True)
            
            # Создаем общий индексный отчет
            index_report = f"""# Отчеты по совместимости YDB

Сгенерировано: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Общая статистика

- Всего тестов: {ai_ready_data['summary_stats']['total_tests']}
- Упавших тестов: {ai_ready_data['summary_stats']['failed_tests_count']}
- mute тестов: {ai_ready_data['summary_stats']['mute_tests_count']}
- Успешность: {ai_ready_data['summary_stats']['success_rate']:.1f}%

## Отчеты по версиям

"""
            
            generated_reports = []
            
            # Генерируем отчеты для каждой версии
            for version, version_data in ai_ready_data['by_version'].items():
                logging.debug(f"Processing version: {version}")
                
                # Проверяем, есть ли данные для этой версии
                total_tests = sum(len(type_data.get('all_tests', [])) for type_data in version_data.values())
                if total_tests == 0:
                    logging.debug(f"Skipping version {version} - no tests found")
                    continue
                
                try:
                    report_path = generate_version_report(version, version_data, ai_ready_data, reports_dir)
                    report_filename = os.path.basename(report_path)
                    generated_reports.append((version, report_filename, total_tests))
                    
                    # Добавляем в индекс
                    index_report += f"- [{version}](./{report_filename}) - {total_tests} тестов\n"
                    
                except Exception as e:
                    logging.error(f"Failed to generate report for version {version}: {e}")
                    index_report += f"- {version} - ОШИБКА ГЕНЕРАЦИИ: {e}\n"
            
            # Сохраняем индексный отчет
            index_path = os.path.join(reports_dir, "README.md")
            with open(index_path, 'w', encoding='utf-8') as f:
                f.write(index_report)
            
            logging.info(f"Сгенерировано {len(generated_reports)} отчетов по версиям в: {reports_dir}")
            logging.info(f"Индексный отчет: {index_path}")
            
            # ===== ГЕНЕРАЦИЯ ОТЧЕТОВ СРАВНЕНИЯ ВЕРСИЙ =====
            logging.info("=== ГЕНЕРАЦИЯ ОТЧЕТОВ СРАВНЕНИЯ ВЕРСИЙ ===")
            comparison_reports = []  # Инициализируем переменную
            
            try:
                comparison_reports = generate_version_comparison_reports(filtered_grouped_data, reports_dir)
            except Exception as e:
                logging.error(f"Ошибка при генерации отчетов сравнения версий: {e}")
                comparison_reports = []  # Устанавливаем пустой список в случае ошибки
            
            # Обновляем индексный отчет с информацией о сравнениях
            if comparison_reports:
                updated_index_report = index_report + f"""

## Отчеты сравнения версий

Отчеты показывающие регрессии и улучшения между версиями:

"""
                for version_a, version_b, report_path in comparison_reports:
                    report_filename = os.path.basename(report_path)
                    updated_index_report += f"- [{version_a} vs {version_b}](./{report_filename})\n"
                
                updated_index_report += f"\n[📋 Полный индекс сравнений](./version_comparisons_index.md)\n"
                
                # Перезаписываем индексный отчет
                with open(index_path, 'w', encoding='utf-8') as f:
                    f.write(updated_index_report)
            
            logging.info(f"Сгенерировано {len(generated_reports)} отчетов по версиям в: {reports_dir}")
            logging.info(f"Сгенерировано {len(comparison_reports)} отчетов сравнения версий")
            logging.info(f"Индексный отчет: {index_path}")
            
            # Выводим сводку
            print(f"\n{'='*60}")
            print(f"ОТЧЕТЫ ПО СОВМЕСТИМОСТИ СГЕНЕРИРОВАНЫ")
            print(f"{'='*60}")
            print(f"Папка с отчетами: {reports_dir}")
            print(f"Индексный файл: {index_path}")
            print(f"\nСгенерировано отчетов по версиям: {len(generated_reports)}")
            for version, filename, tests_count in generated_reports:
                print(f"  - {version}: {filename} ({tests_count} тестов)")
            
            if comparison_reports:
                print(f"\nСгенерировано отчетов сравнения версий: {len(comparison_reports)}")
                for version_a, version_b, report_path in comparison_reports[:5]:  # Показываем первые 5
                    report_filename = os.path.basename(report_path)
                    print(f"  - {version_a} vs {version_b}: {report_filename}")
                if len(comparison_reports) > 5:
                    print(f"  - ... и еще {len(comparison_reports) - 5} сравнений")
                print(f"  📋 Полный список: version_comparisons_index.md")
            
            print(f"{'='*60}")
            
            return 0
    
    except Exception as e:
        logging.error(f"Не удалось подключиться к БД: {e}")
        return 1


def format_error_for_html_table(error_description, max_length=600, log_url=None):
    """Форматирует ошибку для отображения в HTML таблице с переносами строк"""
    if not error_description:
        return 'No error description'
    
    # Получаем содержательную ошибку
    meaningful_error = extract_meaningful_error_info(error_description, max_length, log_url=log_url)
    
    # Заменяем переносы строк на HTML <br> теги
    html_formatted = meaningful_error.replace('\n', '<br>')
    
    # Экранируем специальные символы для markdown таблиц
    html_formatted = html_formatted.replace('|', '&#124;')  # Экранируем вертикальные черты
    
    return html_formatted


def normalize_error_for_grouping(error_description):
    """Нормализует ошибку для группировки похожих ошибок"""
    if not error_description:
        return "no_error"
    
    # Получаем содержательную ошибку
    meaningful_error = extract_meaningful_error_info(error_description, max_length=500)
    
    # Дополнительная нормализация для группировки
    normalized = meaningful_error.lower()
    
    # Убираем специфичные детали, оставляя суть ошибки
    normalized = re.sub(r'\[timestamp\].*?-', '', normalized)  # Убираем временные метки с префиксами
    normalized = re.sub(r'\[build_path\]', '', normalized)     # Убираем пути
    normalized = re.sub(r'\[port\]', '', normalized)           # Убираем порты
    normalized = re.sub(r'\[pid\]', '', normalized)            # Убираем PID
    normalized = re.sub(r'\[hash\]', '', normalized)           # Убираем хеши
    normalized = re.sub(r'\[id\]', '', normalized)             # Убираем ID
    normalized = re.sub(r'\[ip\]', '', normalized)             # Убираем IP
    normalized = re.sub(r'\[n\]', '', normalized)              # Убираем номера узлов
    
    # Убираем лишние пробелы и пунктуацию
    normalized = re.sub(r'\s+', ' ', normalized)
    normalized = re.sub(r'[^\w\s]', ' ', normalized)
    normalized = normalized.strip()
    
    # Берем первые 100 символов как ключ группировки
    return normalized[:100] if normalized else "unknown_error"


def group_tests_by_error(tests):
    """Группирует тесты по похожим ошибкам"""
    error_groups = {}
    
    for test in tests:
        error_key = normalize_error_for_grouping(test.get('error_description', ''))
        
        if error_key not in error_groups:
            error_groups[error_key] = {
                'tests': [],
                'representative_error': test.get('error_description', ''),
                'error_summary': extract_meaningful_error_info(test.get('error_description', ''), max_length=200)
            }
        
        error_groups[error_key]['tests'].append(test)
    
    return error_groups


def should_include_version_in_reports(version):
    """
    Определяет, должна ли версия включаться в отчеты.
    Исключает тестовые, промежуточные и экспериментальные версии.
    """
    if not version or version == 'unknown':
        return False
    
    # Исключаем явно тестовые версии
    test_version_patterns = [
        r'test',
        r'dev',
        r'experimental',
        r'temp',
        r'tmp',
        r'debug',
        r'draft',
        r'wip',  # work in progress
        r'branch',
        r'feature',
        r'fix',
        r'hotfix',
    ]
    
    version_lower = version.lower()
    for pattern in test_version_patterns:
        if re.search(pattern, version_lower):
            logging.debug(f"Excluding test version from reports: {version}")
            return False
    
    # Исключаем версии с очень длинными хешами
    if len(version) > 20 and re.match(r'^[a-f0-9]+$', version):
        logging.debug(f"Excluding hash-like version from reports: {version}")
        return False
    
    # Включаем стандартные версии
    stable_version_patterns = [
        r'^current$',
        r'^trunk$',
        r'^main$',
        r'^\d+-\d+(?:-\d+)?$',  # 25-1, 25-1-3
        r'^\d+\.\d+(?:\.\d+)?$',  # 1.2, 1.2.3
        r'^v\d+(?:\.\d+)*$',  # v1, v1.2, v2.1
    ]
    
    for pattern in stable_version_patterns:
        if re.match(pattern, version):
            return True
    
    # Если версия не подошла под известные паттерны, логируем и исключаем
    logging.warning(f"Unknown version format, excluding from reports: {version}")
    return False

def filter_versions_for_reports(grouped_data):
    """
    Фильтрует версии, оставляя только подходящие для отчетов
    """
    filtered_data = {}
    excluded_versions = []
    
    for version, version_data in grouped_data.items():
        if should_include_version_in_reports(version):
            filtered_data[version] = version_data
        else:
            excluded_versions.append(version)
    
    if excluded_versions:
        logging.info(f"Исключено версий из отчетов: {len(excluded_versions)}")
        logging.info(f"Исключенные версии: {excluded_versions}")
    
    logging.info(f"Версии для отчетов: {sorted(filtered_data.keys())}")
    
    return filtered_data


def is_xml_description_sufficient(xml_description):
    """
    Проверяет, достаточно ли информативно XML описание ошибки для failure тестов.
    
    Возвращает True если XML описание содержит достаточно информации,
    False если нужна дополнительная обработка лога.
    """
    if not xml_description or len(xml_description.strip()) < 50:
        return False
    
    # Недостаточные описания - требуют дополнительной обработки
    insufficient_patterns = [
        r'^ExecutionError: Command.*has failed with code \d+$',  # Только код ошибки
        r'^Not enough information in log',  # Прямое указание недостатка информации
        r'^Command.*failed with exit code \d+$',  # Только exit code
        r'^yatest\.common\.process\.ExecutionError:.*failed with code \d+$',  # yatest ошибка с кодом
        r'^Test execution failed$',  # Общая фраза
        r'^Process finished with exit code \d+$',  # Только exit code
        r'^Execution failed$',  # Слишком общее
    ]
    
    xml_stripped = xml_description.strip()
    
    # Проверяем на недостаточные паттерны
    for pattern in insufficient_patterns:
        if re.match(pattern, xml_stripped):
            return False
    
    # Достаточные описания содержат конкретную информацию об ошибке
    sufficient_indicators = [
        'SeveralDaemonErrors:',  # Daemon crashes
        'Daemon failed with message:',  # Daemon ошибки
        'VERIFY failed',  # Memory verification
        'Function.*type mismatch',  # Type errors
        'Mkql memory limit exceeded',  # Memory errors
        'Unknown node in OLAP',  # OLAP errors
        'unknown field',  # Config errors
        'timeout.*exceeded',  # Timeout errors
        'assertion.*failed',  # Assertion errors
        'requirement.*failed',  # Requirement errors
        'KiKiMR start failed',  # Startup errors
    ]
    
    xml_lower = xml_stripped.lower()
    for indicator in sufficient_indicators:
        if re.search(indicator.lower(), xml_lower):
            return True
    
    # Если описание длинное и содержательное (более 200 символов), считаем достаточным
    if len(xml_stripped) > 200:
        return True
    
    return False


def normalize_failure_status_description(test_data):
    """
    Нормализует status_description у failure тестов:
    заменяет ';;' на '\n' для правильной обработки парсерами ошибок
    """
    logging.debug("=== НОРМАЛИЗАЦИЯ STATUS_DESCRIPTION У FAILURE ТЕСТОВ ===")
    
    failure_tests = [r for r in test_data if r.get('status') == 'failure']
    normalized_count = 0
    
    for record in failure_tests:
        status_description = record.get('status_description', '')
        if status_description and ';;' in status_description:
            # Заменяем ';;' на '\n' для правильной обработки парсерами
            normalized_description = status_description.replace(';;', '\n')
            record['status_description'] = normalized_description
            normalized_count += 1
            
            logging.debug(f"Нормализован failure тест: {record.get('test_name', '')}")
            logging.debug(f"  До: {status_description[:100]}...")
            logging.debug(f"  После: {normalized_description[:100]}...")
    
    logging.info(f"Нормализовано {normalized_count} failure тестов (заменено ';;' на '\\n')")
    return test_data


def generate_version_comparison_reports(grouped_data, output_dir):
    """
    Генерирует отчеты сравнения версий - показывает тесты которые проходили в версии A, но падают в версии B
    """
    logging.info("=== ГЕНЕРАЦИЯ ОТЧЕТОВ СРАВНЕНИЯ ВЕРСИЙ ===")
    
    # Получаем список всех версий
    versions = list(grouped_data.keys())
    versions = [v for v in versions if should_include_version_in_reports(v)]
    versions.sort()
    
    logging.info(f"Генерируем сравнения для {len(versions)} версий: {versions}")
    
    comparison_reports = []
    
    # Генерируем сравнения для всех пар версий
    for i, version_a in enumerate(versions):
        for j, version_b in enumerate(versions):
            if i >= j:  # Избегаем дублирования и самосравнения
                continue
                
            logging.debug(f"Сравниваем {version_a} vs {version_b}")
            
            try:
                report_path = generate_single_version_comparison(
                    version_a, version_b, grouped_data, output_dir
                )
                if report_path:
                    comparison_reports.append((version_a, version_b, report_path))
                    logging.debug(f"Создан отчет сравнения: {report_path}")
            except Exception as e:
                logging.error(f"Ошибка при создании сравнения {version_a} vs {version_b}: {e}")
    
    # Создаем индексный файл для сравнений
    if comparison_reports:
        index_content = f"""# Отчеты сравнения версий YDB

Сгенерировано: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

Эти отчеты показывают тесты, которые проходили в одной версии, но падают в другой.

## Доступные сравнения

"""
        
        for version_a, version_b, report_path in comparison_reports:
            report_filename = os.path.basename(report_path)
            index_content += f"- [{version_a} vs {version_b}](./{report_filename})\n"
        
        index_path = os.path.join(output_dir, "version_comparisons_index.md")
        with open(index_path, 'w', encoding='utf-8') as f:
            f.write(index_content)
        
        logging.info(f"Создан индекс сравнений: {index_path}")
        logging.info(f"Сгенерировано {len(comparison_reports)} отчетов сравнения версий")
    
    return comparison_reports


def generate_single_version_comparison(version_a, version_b, grouped_data, output_dir):
    """
    Генерирует отчет сравнения двух конкретных версий
    """
    data_a = grouped_data.get(version_a, {})
    data_b = grouped_data.get(version_b, {})
    
    if not data_a or not data_b:
        logging.debug(f"Недостаточно данных для сравнения {version_a} vs {version_b}")
        return None
    
    # Собираем все тесты из обеих версий
    tests_a = {}  # normalized_test_name -> test_info
    tests_b = {}
    
    for test_type, type_data in data_a.items():
        for test_name, runs in type_data.items():
            if runs:  # Если есть запуски
                latest_run = runs[0]  # Последний запуск
                # Нормализуем имя теста для сравнения
                normalized_name = normalize_test_name_for_comparison(test_name)
                tests_a[normalized_name] = {
                    'original_name': test_name,  # Сохраняем оригинальное имя
                    'status': latest_run.get('status', 'unknown'),
                    'test_context': latest_run.get('test_context', ''),
                    'error_description': latest_run.get('status_description', ''),
                    'log_url': latest_run.get('log', ''),
                    'test_type': test_type,
                    'last_run_time': latest_run.get('run_timestamp')  # Добавляем время последнего запуска
                }
    
    for test_type, type_data in data_b.items():
        for test_name, runs in type_data.items():
            if runs:  # Если есть запуски
                latest_run = runs[0]  # Последний запуск
                # Нормализуем имя теста для сравнения
                normalized_name = normalize_test_name_for_comparison(test_name)
                tests_b[normalized_name] = {
                    'original_name': test_name,  # Сохраняем оригинальное имя
                    'status': latest_run.get('status', 'unknown'),
                    'test_context': latest_run.get('test_context', ''),
                    'error_description': latest_run.get('status_description', ''),
                    'log_url': latest_run.get('log', ''),
                    'test_type': test_type,
                    'last_run_time': latest_run.get('run_timestamp')  # Добавляем время последнего запуска
                }
    
    # Находим регрессии: тесты которые проходили в A, но падают в B
    regressions = []
    improvements = []
    common_failures = []
    compatibility_tests = []  # Новая категория для тестов совместимости
    skipped_tests = []  # Инициализируем список пропущенных тестов
    
    for normalized_name in set(tests_a.keys()) & set(tests_b.keys()):
        status_a = tests_a[normalized_name]['status']
        status_b = tests_b[normalized_name]['status']
        
        # Проверяем, является ли это тестом совместимости между сравниваемыми версиями
        test_name_a = tests_a[normalized_name]['original_name']
        test_name_b = tests_b[normalized_name]['original_name']
        
        is_compat_test = (is_compatibility_test_for_versions(test_name_a, version_a, version_b) or 
                         is_compatibility_test_for_versions(test_name_b, version_a, version_b))
        
        # Если тест пропущен (skipped) в любой из версий
        if status_a == 'skipped' or status_b == 'skipped':
            # Выбираем информацию об ошибке из версии, где тест пропущен
            error_description = ''
            log_url = ''
            
            if status_a == 'skipped':
                error_description = tests_a[normalized_name]['error_description']
                log_url = tests_a[normalized_name]['log_url']
            elif status_b == 'skipped':
                error_description = tests_b[normalized_name]['error_description']
                log_url = tests_b[normalized_name]['log_url']
            
            skipped_tests.append({
                'test_name': tests_b[normalized_name]['original_name'],
                'version_a_status': status_a,
                'version_b_status': status_b,
                'test_context': tests_b[normalized_name]['test_context'],
                'error_description': error_description,
                'log_url': log_url
            })
        
        if is_compat_test:
            # Это тест совместимости - добавляем в отдельную категорию
            compatibility_tests.append({
                'test_name': test_name_b,  # Используем имя из version_b
                'normalized_name': normalized_name,
                'version_a_name': test_name_a,
                'version_a_status': status_a,
                'version_b_status': status_b,
                'test_context': tests_b[normalized_name]['test_context'],
                'error_description': tests_b[normalized_name]['error_description'],
                'log_url': tests_b[normalized_name]['log_url'],
                'test_type': tests_b[normalized_name]['test_type'],
                'last_run_time': tests_b[normalized_name].get('last_run_time')  # Добавляем время последнего запуска
            })
        else:
            # Обычные тесты - классифицируем как раньше
            # Регрессии: passed -> failure/mute
            if status_a == 'passed' and status_b in ['failure', 'mute']:
                regressions.append({
                    'test_name': tests_b[normalized_name]['original_name'],  # Используем оригинальное имя из version_b
                    'normalized_name': normalized_name,
                    'version_a_name': tests_a[normalized_name]['original_name'],
                    'version_a_status': status_a,
                    'version_b_status': status_b,
                    'test_context': tests_b[normalized_name]['test_context'],
                    'error_description': tests_b[normalized_name]['error_description'],
                    'log_url': tests_b[normalized_name]['log_url'],
                    'test_type': tests_b[normalized_name]['test_type'],
                    'last_run_time': tests_b[normalized_name].get('last_run_time')  # Добавляем время последнего запуска
                })
            
            # Улучшения: failure/mute -> passed
            elif status_a in ['failure', 'mute'] and status_b == 'passed':
                improvements.append({
                    'test_name': tests_a[normalized_name]['original_name'],  # Используем оригинальное имя из version_a
                    'normalized_name': normalized_name,
                    'version_b_name': tests_b[normalized_name]['original_name'],
                    'version_a_status': status_a,
                    'version_b_status': status_b,
                    'test_context': tests_a[normalized_name]['test_context'],
                    'error_description': tests_a[normalized_name]['error_description'],
                    'log_url': tests_a[normalized_name]['log_url'],
                    'test_type': tests_a[normalized_name]['test_type'],
                    'last_run_time': tests_a[normalized_name].get('last_run_time')  # Добавляем время последнего запуска
                })
            
            # Общие падения: failure/mute в обеих версиях
            elif status_a in ['failure', 'mute'] and status_b in ['failure', 'mute']:
                common_failures.append({
                    'test_name': tests_b[normalized_name]['original_name'],  # Используем оригинальное имя из version_b
                    'normalized_name': normalized_name,
                    'version_a_name': tests_a[normalized_name]['original_name'],
                    'version_a_status': status_a,
                    'version_b_status': status_b,
                    'test_context': tests_b[normalized_name]['test_context'],
                    'error_description': tests_b[normalized_name]['error_description'],
                    'log_url': tests_b[normalized_name]['log_url'],
                    'test_type': tests_b[normalized_name]['test_type'],
                    'last_run_time': tests_b[normalized_name].get('last_run_time')  # Добавляем время последнего запуска
                })
    
    # Всегда создаем отчет для показа полного diff между версиями
    logging.debug(f"Создаем отчет сравнения {version_a} vs {version_b}: регрессий={len(regressions)}, улучшений={len(improvements)}, общих падений={len(common_failures)}, тестов совместимости={len(compatibility_tests)}")
    
    # Находим тесты, которые есть только в одной версии (по нормализованным именам)
    only_in_a = []
    only_in_b = []
    
    for normalized_name in set(tests_a.keys()) - set(tests_b.keys()):
        test_info = tests_a[normalized_name]
        only_in_a.append({
            'test_name': test_info['original_name'],
            'normalized_name': normalized_name,
            'status': test_info['status'],
            'test_context': test_info.get('test_context', ''),
            'error_description': test_info.get('error_description', ''),
            'log_url': test_info.get('log_url', ''),
            'test_type': test_info['test_type'],
            'last_run_time': test_info.get('last_run_time')  # Добавляем время последнего запуска
        })
    
    for normalized_name in set(tests_b.keys()) - set(tests_a.keys()):
        test_info = tests_b[normalized_name]
        only_in_b.append({
            'test_name': test_info['original_name'],
            'normalized_name': normalized_name,
            'status': test_info['status'],
            'test_context': test_info.get('test_context', ''),
            'error_description': test_info.get('error_description', ''),
            'log_url': test_info.get('log_url', ''),
            'test_type': test_info['test_type'],
            'last_run_time': test_info.get('last_run_time')  # Добавляем время последнего запуска
        })
    
    # Находим тесты без изменений статуса
    unchanged_tests = []
    not_run_recently = []  # Инициализируем список тестов, которые не запускались недавно
    
    for normalized_name in set(tests_a.keys()) & set(tests_b.keys()):
        status_a = tests_a[normalized_name]['status']
        status_b = tests_b[normalized_name]['status']
        
        # Если статус не изменился (не регрессия, не улучшение, не общее падение)
        if not (
            (status_a == 'passed' and status_b in ['failure', 'mute']) or  # регрессия
            (status_a in ['failure', 'mute'] and status_b == 'passed') or  # улучшение
            (status_a in ['failure', 'mute'] and status_b in ['failure', 'mute'])  # общее падение
        ):
            unchanged_tests.append({
                'test_name': tests_b[normalized_name]['original_name'],  # Используем оригинальное имя из version_b
                'normalized_name': normalized_name,
                'version_a_name': tests_a[normalized_name]['original_name'],
                'status': status_b,  # статус одинаковый в обеих версиях
                'test_context': tests_b[normalized_name].get('test_context', ''),
                'test_type': tests_b[normalized_name]['test_type'],
                'last_run_time': tests_b[normalized_name].get('last_run_time')  # Добавляем время последнего запуска
            })
    
    # Генерируем отчет
    report_content = f"""# Сравнение версий: {version_a} vs {version_b}

**Сгенерировано:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## 📊 Сводка изменений

| Метрика | Количество |
|---------|------------|
| 📈 **Регрессии** (проходили → падают) | **{len(regressions)}** |
| 📉 **Улучшения** (падали → проходят) | **{len(improvements)}** |
| 🔄 **Общие падения** (падают в обеих) | {len(common_failures)} |
| 🔗 **Тесты совместимости** ({version_a} ↔ {version_b}) | {len(compatibility_tests)} |
| ⏩ **Пропущенные тесты** | {len(skipped_tests)} |
| ⏰ **Не запускались >24ч** | {len(not_run_recently)} |
| ➡️ **Без изменений** | {len(unchanged_tests)} |
| ➕ **Только в {version_a}** | {len(only_in_a)} |
| ➖ **Только в {version_b}** | {len(only_in_b)} |

"""
    
    # Определяем общий тренд
    if len(regressions) > len(improvements):
        trend_emoji = "📈❌"
        trend_text = "Ухудшение"
    elif len(improvements) > len(regressions):
        trend_emoji = "📉✅"
        trend_text = "Улучшение"
    else:
        trend_emoji = "➡️"
        trend_text = "Без изменений"
    
    report_content += f"""
**Общий тренд:** {trend_emoji} {trend_text}

---

"""
    
    # Секция регрессий
    if regressions:
        report_content += f"""## 📈❌ Регрессии ({len(regressions)})
*Тесты, которые проходили в {version_a}, но падают в {version_b}*

| Тест | Было ({version_a}) | Стало ({version_b}) | Контекст | Error Pattern | Ошибка |
|------|-------------------|---------------------|----------|---------|--------|
"""
        
        # Сортируем регрессии по имени теста
        sorted_regressions = sorted(regressions, key=lambda x: x['test_name'])
        
        # Находим общий префикс путей для возможного сокращения
        if len(sorted_regressions) > 0:
            common_prefix = os.path.commonprefix([r['test_name'] for r in sorted_regressions])
            if not common_prefix.endswith('/'):
                common_prefix = common_prefix.rsplit('/', 1)[0] + '/' if '/' in common_prefix else ''
        else:
            common_prefix = ''
        
        for reg in sorted_regressions:
            # Форматируем ошибку для отображения
            error_formatted = format_error_for_html_table(
                reg['error_description'], max_length=300, log_url=reg['log_url']
            )
            
            # Получаем паттерн ошибки
            error_pattern = format_error_pattern_for_table(
                reg['error_description'], log_url=reg['log_url']
            )
            
            # Опционально сокращаем путь для лучшей читаемости
            if len(common_prefix) > 10:  # Если есть значимый общий префикс
                display_name = reg['test_name'][len(common_prefix):]
            else:
                display_name = reg['test_name']
                
            report_content += f"| {display_name} | ✅ {reg['version_a_status']} | ❌ {reg['version_b_status']} | {reg['test_context']} | {error_pattern} | {error_formatted} |\n"
    
    # Секция улучшений
    if improvements:
        report_content += f"""
## 📉✅ Улучшения ({len(improvements)})
*Тесты, которые падали в {version_a}, но проходят в {version_b}*

| Тест | Было ({version_a}) | Стало ({version_b}) | Контекст | Error Pattern | Предыдущая ошибка |
|------|-------------------|---------------------|----------|---------|-------------------|
"""
        
        # Сортируем улучшения по имени теста
        sorted_improvements = sorted(improvements, key=lambda x: x['test_name'])
        
        # Находим общий префикс путей для возможного сокращения
        if len(sorted_improvements) > 0:
            common_prefix = os.path.commonprefix([i['test_name'] for i in sorted_improvements])
            if not common_prefix.endswith('/'):
                common_prefix = common_prefix.rsplit('/', 1)[0] + '/' if '/' in common_prefix else ''
        else:
            common_prefix = ''
        
        for imp in sorted_improvements:
            # Форматируем ошибку для отображения
            error_formatted = format_error_for_html_table(
                imp['error_description'], max_length=300, log_url=imp['log_url']
            )
            
            # Получаем паттерн ошибки
            error_pattern = format_error_pattern_for_table(
                imp['error_description'], log_url=imp['log_url']
            )
            
            # Опционально сокращаем путь для лучшей читаемости
            if len(common_prefix) > 10:  # Если есть значимый общий префикс
                display_name = imp['test_name'][len(common_prefix):]
            else:
                display_name = imp['test_name']
                
            report_content += f"| {display_name} | ❌ {imp['version_a_status']} | ✅ {imp['version_b_status']} | {imp['test_context']} | {error_pattern} | {error_formatted} |\n"
    
    # Секция тестов совместимости
    if compatibility_tests:
        report_content += f"""
## 🔗 Тесты совместимости {version_a} ↔ {version_b} ({len(compatibility_tests)})
*Тесты, которые специально проверяют совместимость между сравниваемыми версиями*

| Тест | {version_a} | {version_b} | Контекст | Ошибка (если есть) |
|------|-------------|-------------|----------|-------------------|
"""
        
        # Сортируем тесты совместимости по имени теста
        sorted_compat_tests = sorted(compatibility_tests, key=lambda x: x['test_name'])
        
        # Находим общий префикс путей для возможного сокращения
        if len(sorted_compat_tests) > 0:
            common_prefix = os.path.commonprefix([c['test_name'] for c in sorted_compat_tests])
            if not common_prefix.endswith('/'):
                common_prefix = common_prefix.rsplit('/', 1)[0] + '/' if '/' in common_prefix else ''
        else:
            common_prefix = ''
        
        for compat in sorted_compat_tests:
            error_formatted = format_error_pattern_for_table(
                compat['error_description'], log_url=compat['log_url']
            ) if compat['error_description'] and compat['version_b_status'] in ['failure', 'mute'] else 'N/A'
            
            status_a_emoji = '✅' if compat['version_a_status'] == 'passed' else '❌'
            status_b_emoji = '✅' if compat['version_b_status'] == 'passed' else '❌'
            
            # Опционально сокращаем путь для лучшей читаемости
            if len(common_prefix) > 10:  # Если есть значимый общий префикс
                display_name = compat['test_name'][len(common_prefix):]
            else:
                display_name = compat['test_name']
                
            report_content += f"| {display_name} | {status_a_emoji} {compat['version_a_status']} | {status_b_emoji} {compat['version_b_status']} | {compat['test_context']} | {error_formatted} |\n"
        
        # Добавляем анализ тестов совместимости
        passed_compat = len([t for t in compatibility_tests if t['version_b_status'] == 'passed'])
        failed_compat = len(compatibility_tests) - passed_compat
        
        if failed_compat > 0:
            report_content += f"""

⚠️ **Внимание**: {failed_compat} из {len(compatibility_tests)} тестов совместимости падают!
Это может указывать на проблемы совместимости между версиями {version_a} и {version_b}.

"""
        else:
            report_content += f"""

✅ **Отлично**: Все {len(compatibility_tests)} тестов совместимости проходят успешно.

"""
    
    # Секция общих падений (только если есть регрессии или улучшения)
    if common_failures and (regressions or improvements):
        report_content += f"""
## 🔄 Общие падения ({len(common_failures)})
*Тесты, которые падают в обеих версиях*

| Тест | {version_a} | {version_b} | Контекст | Error Pattern | Ошибка в {version_b} |
|------|-------------|-------------|----------|---------|---------------------|
"""
        
        # Сортируем общие падения по имени теста
        sorted_common_failures = sorted(common_failures, key=lambda x: x['test_name'])
        
        # Находим общий префикс путей для возможного сокращения
        if len(sorted_common_failures) > 0:
            common_prefix = os.path.commonprefix([c['test_name'] for c in sorted_common_failures])
            if not common_prefix.endswith('/'):
                common_prefix = common_prefix.rsplit('/', 1)[0] + '/' if '/' in common_prefix else ''
        else:
            common_prefix = ''
        
        # Показываем все общие падения (убираем ограничение в 10)
        for common in sorted_common_failures:
            # Форматируем ошибку для отображения
            error_formatted = format_error_for_html_table(
                common['error_description'], max_length=200, log_url=common['log_url']
            )
            
            # Получаем паттерн ошибки
            error_pattern = format_error_pattern_for_table(
                common['error_description'], log_url=common['log_url']
            )
            
            # Опционально сокращаем путь для лучшей читаемости
            if len(common_prefix) > 10:  # Если есть значимый общий префикс
                display_name = common['test_name'][len(common_prefix):]
            else:
                display_name = common['test_name']
                
            report_content += f"| {display_name} | ❌ {common['version_a_status']} | ❌ {common['version_b_status']} | {common['test_context']} | {error_pattern} | {error_formatted} |\n"
    
    # Секция пропущенных тестов (skipped)
    if skipped_tests:
        report_content += f"""
## ⏩ Тесты не выполнялись ({len(skipped_tests)})
*Тесты, которые были пропущены (skipped) в одной или обеих версиях*

| Тест | {version_a} | {version_b} | Контекст | Причина пропуска |
|------|-------------|-------------|----------|-----------------|
"""
        
        # Сортируем пропущенные тесты по имени
        sorted_skipped = sorted(skipped_tests, key=lambda x: x['test_name'])
        
        # Находим общий префикс путей для возможного сокращения
        if len(sorted_skipped) > 0:
            common_prefix = os.path.commonprefix([s['test_name'] for s in sorted_skipped])
            if not common_prefix.endswith('/'):
                common_prefix = common_prefix.rsplit('/', 1)[0] + '/' if '/' in common_prefix else ''
        else:
            common_prefix = ''
        
        for skip in sorted_skipped:
            status_a_emoji = '✅' if skip['version_a_status'] == 'passed' else '⏩' if skip['version_a_status'] == 'skipped' else '❌'
            status_b_emoji = '✅' if skip['version_b_status'] == 'passed' else '⏩' if skip['version_b_status'] == 'skipped' else '❌'
            
            # Опционально сокращаем путь для лучшей читаемости
            if len(common_prefix) > 10:  # Если есть значимый общий префикс
                display_name = skip['test_name'][len(common_prefix):]
            else:
                display_name = skip['test_name']
            
            # Форматируем причину пропуска
            skip_reason = format_error_pattern_for_table(skip.get('error_description', ''), skip.get('log_url', ''))
                
            report_content += f"| {display_name} | {status_a_emoji} {skip['version_a_status']} | {status_b_emoji} {skip['version_b_status']} | {skip['test_context']} | {skip_reason} |\n"
    
    # Секция тестов, которые не выполнялись более 24 часов
    not_run_recently = []
    now = datetime.now()
    
    # Проверяем тесты в обеих версиях
    for normalized_name in set(tests_a.keys()) & set(tests_b.keys()):
        test_a = tests_a[normalized_name]
        test_b = tests_b[normalized_name]
        
        # Проверяем время последнего запуска, если оно доступно
        last_run_time_a = test_a.get('last_run_time')
        last_run_time_b = test_b.get('last_run_time')
        
        if last_run_time_a and isinstance(last_run_time_a, datetime):
            time_since_run_a = now - last_run_time_a
            if time_since_run_a.total_seconds() > 86400:  # Более 24 часов
                not_run_recently.append({
                    'test_name': test_a['original_name'],
                    'version': version_a,
                    'last_run': last_run_time_a,
                    'time_since': time_since_run_a,
                    'test_context': test_a.get('test_context', '')
                })
        
        if last_run_time_b and isinstance(last_run_time_b, datetime):
            time_since_run_b = now - last_run_time_b
            if time_since_run_b.total_seconds() > 86400:  # Более 24 часов
                not_run_recently.append({
                    'test_name': test_b['original_name'],
                    'version': version_b,
                    'last_run': last_run_time_b,
                    'time_since': time_since_run_b,
                    'test_context': test_b.get('test_context', '')
                })
    
    if not_run_recently:
        report_content += f"""
## ⏰ Не тестировались какое-то время ({len(not_run_recently)})
*Тесты, которые не запускались более 24 часов*

| Тест | Версия | Последний запуск | Контекст |
|------|--------|------------------|----------|
"""
        
        # Сортируем по имени теста и затем по версии
        sorted_not_run = sorted(not_run_recently, key=lambda x: (x['test_name'], x['version']))
        
        # Находим общий префикс путей для возможного сокращения
        if len(sorted_not_run) > 0:
            common_prefix = os.path.commonprefix([n['test_name'] for n in sorted_not_run])
            if not common_prefix.endswith('/'):
                common_prefix = common_prefix.rsplit('/', 1)[0] + '/' if '/' in common_prefix else ''
        else:
            common_prefix = ''
        
        for test in sorted_not_run:
            # Форматируем время в человекочитаемом виде
            days = test['time_since'].days
            hours = test['time_since'].seconds // 3600
            time_str = f"{days} дн. {hours} ч." if days > 0 else f"{hours} ч."
            last_run_str = test['last_run'].strftime("%Y-%m-%d %H:%M")
            
            # Опционально сокращаем путь для лучшей читаемости
            if len(common_prefix) > 10:  # Если есть значимый общий префикс
                display_name = test['test_name'][len(common_prefix):]
            else:
                display_name = test['test_name']
                
            report_content += f"| {display_name} | {test['version']} | {last_run_str} ({time_str} назад) | {test['test_context']} |\n"
    
    # Секция тестов только в version_a
    if only_in_a:
        report_content += f"""
## ➕ Тесты только в {version_a} ({len(only_in_a)})
*Тесты, которые есть в {version_a}, но отсутствуют в {version_b}*

| Тест | Статус | Контекст | Error Pattern | Ошибка |
|------|--------|----------|---------|--------|
"""
        
        # Сортируем тесты только в version_a по имени
        sorted_only_in_a = sorted(only_in_a, key=lambda x: x['test_name'])
        
        # Находим общий префикс путей для возможного сокращения
        if len(sorted_only_in_a) > 0:
            common_prefix = os.path.commonprefix([t['test_name'] for t in sorted_only_in_a])
            if not common_prefix.endswith('/'):
                common_prefix = common_prefix.rsplit('/', 1)[0] + '/' if '/' in common_prefix else ''
        else:
            common_prefix = ''
        
        for test in sorted_only_in_a:
            error_formatted = format_error_for_html_table(
                test['error_description'], max_length=200, log_url=test['log_url']
            ) if test['error_description'] and test['status'] in ['failure', 'mute'] else 'N/A'
            
            # Получаем паттерн ошибки
            error_pattern = format_error_pattern_for_table(
                test['error_description'], log_url=test['log_url']
            ) if test['error_description'] and test['status'] in ['failure', 'mute'] else 'N/A'
            
            # Опционально сокращаем путь для лучшей читаемости
            if len(common_prefix) > 10:  # Если есть значимый общий префикс
                display_name = test['test_name'][len(common_prefix):]
            else:
                display_name = test['test_name']
                
            report_content += f"| {display_name} | {test['status']} | {test['test_context']} | {error_pattern} | {error_formatted} |\n"
    
    # Секция тестов только в version_b
    if only_in_b:
        report_content += f"""
## ➖ Тесты только в {version_b} ({len(only_in_b)})
*Тесты, которые есть в {version_b}, но отсутствуют в {version_a}*

| Тест | Статус | Контекст | Error Pattern | Ошибка |
|------|--------|----------|---------|--------|
"""
        
        # Сортируем тесты только в version_b по имени
        sorted_only_in_b = sorted(only_in_b, key=lambda x: x['test_name'])
        
        # Находим общий префикс путей для возможного сокращения
        if len(sorted_only_in_b) > 0:
            common_prefix = os.path.commonprefix([t['test_name'] for t in sorted_only_in_b])
            if not common_prefix.endswith('/'):
                common_prefix = common_prefix.rsplit('/', 1)[0] + '/' if '/' in common_prefix else ''
        else:
            common_prefix = ''
        
        for test in sorted_only_in_b:
            error_formatted = format_error_for_html_table(
                test['error_description'], max_length=200, log_url=test['log_url']
            ) if test['error_description'] and test['status'] in ['failure', 'mute'] else 'N/A'
            
            # Получаем паттерн ошибки
            error_pattern = format_error_pattern_for_table(
                test['error_description'], log_url=test['log_url']
            ) if test['error_description'] and test['status'] in ['failure', 'mute'] else 'N/A'
            
            # Опционально сокращаем путь для лучшей читаемости
            if len(common_prefix) > 10:  # Если есть значимый общий префикс
                display_name = test['test_name'][len(common_prefix):]
            else:
                display_name = test['test_name']
                
            report_content += f"| {display_name} | {test['status']} | {test['test_context']} | {error_pattern} | {error_formatted} |\n"
    
    # Секция тестов без изменений (только если есть изменения для контраста)
    if unchanged_tests and (regressions or improvements or only_in_a or only_in_b):
        report_content += f"""
## ➡️ Тесты без изменений ({len(unchanged_tests)})
*Тесты со стабильным статусом в обеих версиях*

"""
        
        # Группируем по статусам
        unchanged_by_status = {}
        for test in unchanged_tests:
            status = test['status']
            if status not in unchanged_by_status:
                unchanged_by_status[status] = []
            unchanged_by_status[status].append(test)
        
        report_content += "| Статус | Количество | Примеры |\n|--------|------------|----------|\n"
        
        for status, status_tests in sorted(unchanged_by_status.items(), 
                                         key=lambda x: len(x[1]), reverse=True):
            examples = [test['test_name'].split('/')[-1] for test in status_tests[:3]]
            examples_str = ', '.join(examples)
            if len(status_tests) > 3:
                examples_str += f", ... (+{len(status_tests) - 3})"
            
            status_emoji = '✅' if status == 'passed' else '❌'
            report_content += f"| {status_emoji} {status} | {len(status_tests)} | {examples_str} |\n"
    
    # Анализ по типам тестов
    if regressions:
        report_content += f"""
---

## 📊 Анализ регрессий по типам тестов

"""
        
        # Группируем регрессии по типам тестов
        regressions_by_type = {}
        for reg in regressions:
            test_type = reg['test_type']
            if test_type not in regressions_by_type:
                regressions_by_type[test_type] = []
            regressions_by_type[test_type].append(reg)
        
        report_content += "| Тип теста | Регрессий | Примеры |\n|-----------|-----------|----------|\n"
        
        for test_type, type_regressions in sorted(regressions_by_type.items(), 
                                                key=lambda x: len(x[1]), reverse=True):
            examples = [reg['test_name'].split('/')[-1] for reg in type_regressions[:3]]
            examples_str = ', '.join(examples)
            if len(type_regressions) > 3:
                examples_str += f", ... (+{len(type_regressions) - 3})"
            
            report_content += f"| {test_type} | {len(type_regressions)} | {examples_str} |\n"
    
    # Рекомендации
    report_content += f"""
---

## 🎯 Рекомендации

"""
    
    if regressions:
        report_content += f"- 🔥 **КРИТИЧНО**: Исследовать {len(regressions)} регрессий при переходе с {version_a} на {version_b}\n"
        
        # Анализируем типы ошибок в регрессиях
        error_types = {}
        for reg in regressions:
            error_desc = reg['error_description'].lower()
            if 'verify' in error_desc:
                error_types.setdefault('VERIFY', []).append(reg['test_name'])
            elif 'memory limit' in error_desc or 'mkql memory' in error_desc:
                error_types.setdefault('Memory issues', []).append(reg['test_name'])
            elif 'daemon failed' in error_desc or 'severaldaemonerrors' in error_desc:
                error_types.setdefault('Daemon crashes', []).append(reg['test_name'])
            elif 'type mismatch' in error_desc:
                error_types.setdefault('Type compatibility', []).append(reg['test_name'])
            else:
                error_types.setdefault('Other errors', []).append(reg['test_name'])
        
        if error_types:
            report_content += f"- 📋 **Приоритеты исследования по типам ошибок:**\n"
            for error_type, tests in sorted(error_types.items(), key=lambda x: len(x[1]), reverse=True):
                report_content += f"  - {error_type}: {len(tests)} тестов\n"
    
    if improvements:
        report_content += f"- ✅ **ПОЗИТИВ**: {len(improvements)} тестов исправлено в {version_b}\n"
    
    if compatibility_tests:
        passed_compat = len([t for t in compatibility_tests if t['version_b_status'] == 'passed'])
        failed_compat = len(compatibility_tests) - passed_compat
        
        if failed_compat > 0:
            report_content += f"- 🔗 **СОВМЕСТИМОСТЬ**: {failed_compat} из {len(compatibility_tests)} тестов совместимости падают - требует внимания!\n"
        else:
            report_content += f"- 🔗 **СОВМЕСТИМОСТЬ**: Все {len(compatibility_tests)} тестов совместимости проходят успешно\n"
    
    if only_in_a:
        report_content += f"- ➕ **ВНИМАНИЕ**: {len(only_in_a)} тестов отсутствуют в {version_b} (удалены или переименованы?)\n"
    
    if only_in_b:
        report_content += f"- ➖ **НОВОЕ**: {len(only_in_b)} новых тестов добавлено в {version_b}\n"
    
    if unchanged_tests:
        passed_unchanged = len([t for t in unchanged_tests if t['status'] == 'passed'])
        failed_unchanged = len(unchanged_tests) - passed_unchanged
        if passed_unchanged > 0:
            report_content += f"- ✅ **СТАБИЛЬНОСТЬ**: {passed_unchanged} тестов стабильно проходят в обеих версиях\n"
        if failed_unchanged > 0:
            report_content += f"- ❌ **ТЕХНИЧЕСКИЙ ДОЛГ**: {failed_unchanged} тестов стабильно падают в обеих версиях\n"
    
    if skipped_tests:
        report_content += f"- ⏩ **Пропущенные тесты** | {len(skipped_tests)} |\n"
    
    if not_run_recently:
        report_content += f"- ⏰ **УСТАРЕВШИЕ ДАННЫЕ**: {len(not_run_recently)} тестов не запускались более 24 часов\n"
    
    if not regressions and improvements:
        report_content += f"- 🎉 **ОТЛИЧНО**: Нет регрессий, только улучшения!\n"
    elif not regressions and not improvements and not only_in_a and not only_in_b:
        report_content += f"- 🔄 **СТАБИЛЬНО**: Функциональных изменений между версиями не обнаружено\n"
    
    report_content += f"""
---

*Отчет сравнения версий сгенерирован автоматически*
*Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*
"""
    
    # Сохраняем отчет
    safe_version_a = version_a.replace('/', '_').replace('-', '_')
    safe_version_b = version_b.replace('/', '_').replace('-', '_')
    report_filename = f"comparison_{safe_version_a}_vs_{safe_version_b}.md"
    report_path = os.path.join(output_dir, report_filename)
    
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(report_content)
    
    return report_path


def format_error_pattern_for_table(error_description, log_url=None):
    """
    Форматирует паттерн ошибки для таблицы вместо полного текста.
    Возвращает краткий паттерн ошибки + ссылку на лог.
    """
    if not error_description:
        pattern = 'No error description'
    else:
        # Получаем содержательную ошибку
        meaningful_error = extract_meaningful_error_info(error_description, max_length=500)
        
        # Определяем тип ошибки и создаем краткий паттерн
        error_lower = meaningful_error.lower()
        
        if 'severaldaemonerrors:' in error_lower or 'daemon failed with message:' in error_lower:
            pattern = '🔴 Daemon Crash'
        elif 'verify' in error_lower or 'verifydebug' in error_lower:
            pattern = '🔍 VERIFY'
        elif 'mkql memory limit exceeded' in error_lower:
            pattern = '💾 Memory Limit Exceeded'
        elif 'function' in error_lower and 'type mismatch' in error_lower:
            pattern = '🔀 Type Mismatch'
        elif 'unknown node in olap' in error_lower:
            pattern = '📊 OLAP Node Error'
        elif 'unknown field' in error_lower:
            pattern = '⚙️ Config Field Error'
        elif 'timeout' in error_lower and 'exceeded' in error_lower:
            pattern = '⏱️ Timeout Exceeded'
        elif 'assertion' in error_lower and 'failed' in error_lower:
            pattern = '❌ Assertion Failed'
        elif 'requirement' in error_lower and 'failed' in error_lower:
            pattern = '📋 Requirement Failed'
        elif 'kikimr start failed' in error_lower:
            pattern = '🚀 Startup Failed'
        elif 'executionerror' in error_lower and 'failed with code' in error_lower:
            pattern = '💥 Execution Error'
        elif 'command' in error_lower and 'failed' in error_lower:
            pattern = '⚡ Command Failed'
        else:
            # Для неизвестных ошибок берем первые 50 символов
            # Удаляем переносы строк и экранируем вертикальные черты
            cleaned_error = meaningful_error.replace('\n', ' ').replace('\r', ' ')
            cleaned_error = re.sub(r'\s+', ' ', cleaned_error).strip()
            pattern = f"❓ {cleaned_error[:50]}..."
    
    # Экранируем вертикальные черты для markdown таблиц
    pattern = pattern.replace('|', '\\|')
    
    # Добавляем ссылку на лог если есть
    if log_url:
        return f"{pattern} [📋 Log]({log_url})"
    else:
        return pattern


def format_error_for_html_table(error_description, max_length=600, log_url=None):
    """Форматирует ошибку для отображения в HTML таблице с переносами строк"""
    if not error_description:
        return 'No error description'
    
    # Получаем содержательную ошибку
    meaningful_error = extract_meaningful_error_info(error_description, max_length, log_url=log_url)
    
    # Заменяем переносы строк на пробелы для совместимости с Markdown таблицами
    formatted_error = meaningful_error.replace('\n', ' ').replace('\r', ' ')
    
    # Убираем лишние пробелы
    formatted_error = re.sub(r'\s+', ' ', formatted_error).strip()
    
    # Экранируем специальные символы для markdown таблиц
    formatted_error = formatted_error.replace('|', '\\|')  # Экранируем вертикальные черты
    
    return formatted_error


def normalize_test_name_for_comparison(test_name):
    """
    Нормализует имя теста для сравнения версий, заменяя версии в параметрах на плейсхолдеры.
    
    Примеры:
    test_tpch1[restart_24-4_to_25-1-column-date64] -> test_tpch1[restart_VERSION_A_to_VERSION_B-column-date64]
    test_simple_queue[mixed_current_and_25-1-column] -> test_simple_queue[mixed_current_and_VERSION-column]
    test_simple_queue[mixed_current_and_stable-25-1-2-column] -> test_simple_queue[mixed_current_and_VERSION-column]
    test_example[mixed_25-1] -> test_example[mixed_VERSION]
    test_example[mixed_stable-25-1-2] -> test_example[mixed_VERSION]
    """
    if not test_name or '[' not in test_name:
        return test_name
    
    # Разделяем имя теста и параметры
    parts = test_name.split('[', 1)
    if len(parts) != 2:
        return test_name
    
    base_name = parts[0]
    params = parts[1].rstrip(']')
    
    # Паттерны для замены версий
    import re
    
    # 1. Паттерн restart_VERSION_A_to_VERSION_B (для test_tpch1, test_compatibility)
    # Примеры: restart_24-4_to_25-1-column-date64 -> restart_VERSION_A_to_VERSION_B-column-date64
    # Ограничиваем версии только числовыми компонентами
    params = re.sub(
        r'restart_((?:stable-)?(?:\d+-\d+(?:-\d+)*|current))_to_((?:stable-)?(?:\d+-\d+(?:-\d+)*|current))(.*)',
        r'restart_VERSION_A_to_VERSION_B\3',
        params
    )
    
    # 2. Паттерн mixed_current_and_VERSION (для test_simple_queue, test_stress)  
    # Примеры: mixed_current_and_25-1-column -> mixed_current_and_VERSION-column
    # Ограничиваем версию числовыми компонентами
    params = re.sub(
        r'mixed_current_and_((?:stable-)?(?:\d+-\d+(?:-\d+)*|current))(.*)',
        r'mixed_current_and_VERSION\2',
        params
    )
    
    # 3. Паттерн mixed_VERSION (для test_example и др.)
    # Примеры: mixed_25-1 -> mixed_VERSION, mixed_stable-25-1-2 -> mixed_VERSION
    params = re.sub(
        r'mixed_((?:stable-)?(?:\d+-\d+(?:-\d+)*|current))(?!_and_)(.*)',
        r'mixed_VERSION\2',
        params
    )
    
    # 4. Паттерн mixed_VERSION_and_VERSION (для тестов совместимости)
    # Примеры: mixed_25-1_and_24-4 -> mixed_VERSION_and_VERSION
    # Примеры: mixed_stable-25-1-2_and_stable-24-4 -> mixed_VERSION_and_VERSION
    params = re.sub(
        r'mixed_((?:stable-)?(?:\d+-\d+(?:-\d+)*|current))_and_((?:stable-)?(?:\d+-\d+(?:-\d+)*|current))(.*)',
        r'mixed_VERSION_and_VERSION\3',
        params
    )
    
    # 5. Паттерн rolling_VERSION (для test_batch_update)
    # Примеры: rolling_25-1-1_to_current -> rolling_VERSION
    # Здесь может быть _to_ паттерн или просто версия
    params = re.sub(
        r'rolling_((?:stable-)?(?:\d+-\d+(?:-\d+)*|current)(?:_to_(?:stable-)?(?:\d+-\d+(?:-\d+)*|current))?)(.*)',
        r'rolling_VERSION\2',
        params
    )
    
    # 6. Общий паттерн для оставшихся версий
    # Заменяем отдельно стоящие версии вида X-Y, X-Y-Z, stable-X-Y, stable-X-Y-Z, current
    params = re.sub(
        r'\b(?:stable-)?(?:\d+-\d+(?:-\d+)?|current)\b',
        'VERSION',
        params
    )
    
    return f"{base_name}[{params}]"


def normalize_error_for_grouping(error_description):
    """
    Нормализует ошибку для группировки похожих ошибок
    """
    if not error_description:
        return "no_error"
    
    # Получаем содержательную ошибку
    meaningful_error = extract_meaningful_error_info(error_description, max_length=500)
    
    # Дополнительная нормализация для группировки
    normalized = meaningful_error.lower()
    
    # Убираем специфичные детали, оставляя суть ошибки
    normalized = re.sub(r'\[timestamp\].*?-', '', normalized)  # Убираем временные метки с префиксами
    normalized = re.sub(r'\[build_path\]', '', normalized)     # Убираем пути
    normalized = re.sub(r'\[port\]', '', normalized)           # Убираем порты
    normalized = re.sub(r'\[pid\]', '', normalized)            # Убираем PID
    normalized = re.sub(r'\[hash\]', '', normalized)           # Убираем хеши
    normalized = re.sub(r'\[id\]', '', normalized)             # Убираем ID
    normalized = re.sub(r'\[ip\]', '', normalized)             # Убираем IP
    normalized = re.sub(r'\[n\]', '', normalized)              # Убираем номера узлов
    
    # Убираем лишние пробелы и пунктуацию
    normalized = re.sub(r'\s+', ' ', normalized)
    normalized = re.sub(r'[^\w\s]', ' ', normalized)
    normalized = normalized.strip()
    
    # Берем первые 100 символов как ключ группировки
    return normalized[:100] if normalized else "unknown_error"


def is_compatibility_test_for_versions(test_name, version_a, version_b):
    """
    Проверяет, является ли тест совместимости между указанными версиями.
    
    Примеры:
    test_tpch1[mixed_stable-25-1-2_and_stable-25-1-1-row] при сравнении 25-1-1 vs 25-1-2
    test_compatibility[restart_24-4_to_25-1-column] при сравнении 24-4 vs 25-1
    """
    if not test_name or '[' not in test_name:
        return False
    
    # Извлекаем параметры теста
    parts = test_name.split('[', 1)
    if len(parts) != 2:
        return False
    
    params = parts[1].rstrip(']').lower()
    
    # Нормализуем версии для сравнения (убираем stable- префиксы и лишние символы)
    def normalize_version(v):
        return v.replace('stable-', '').replace('_', '-').strip()
    
    norm_a = normalize_version(version_a)
    norm_b = normalize_version(version_b)
    
    # Паттерны совместимости:
    import re
    
    # 1. mixed_VERSION_and_VERSION
    mixed_pattern = r'mixed_(?:stable-)?([^_]+)_and_(?:stable-)?(\d+-\d+(?:-\d+)*)'
    mixed_match = re.search(mixed_pattern, params)
    if mixed_match:
        test_v1 = normalize_version(mixed_match.group(1))
        test_v2 = normalize_version(mixed_match.group(2))
        # Проверяем, совпадают ли версии в тесте с версиями сравнения (в любом порядке)
        return (test_v1 == norm_a and test_v2 == norm_b) or (test_v1 == norm_b and test_v2 == norm_a)
    
    # 2. restart_VERSION_to_VERSION
    restart_pattern = r'restart_(?:stable-)?(\d+-\d+(?:-\d+)*|current)_to_(?:stable-)?(\d+-\d+(?:-\d+)*|current)'
    restart_match = re.search(restart_pattern, params)
    if restart_match:
        test_v1 = normalize_version(restart_match.group(1))
        test_v2 = normalize_version(restart_match.group(2))
        # Проверяем, совпадают ли версии в тесте с версиями сравнения (в любом порядке)
        return (test_v1 == norm_a and test_v2 == norm_b) or (test_v1 == norm_b and test_v2 == norm_a)
    
    return False


def clean_test_name_for_display(test_name):
    """
    Очищает имя теста от избыточного пути ydb/tests/compatibility/
    """
    if not test_name:
        return test_name
    
    # Убираем префикс ydb/tests/compatibility/
    prefixes_to_remove = [
        'ydb/tests/compatibility/',
        'tests/compatibility/',
        'compatibility/'
    ]
    
    cleaned_name = test_name
    for prefix in prefixes_to_remove:
        if cleaned_name.startswith(prefix):
            cleaned_name = cleaned_name[len(prefix):]
            break
    
    return cleaned_name


if __name__ == "__main__":
    exit(generate_compatibility_report()) 