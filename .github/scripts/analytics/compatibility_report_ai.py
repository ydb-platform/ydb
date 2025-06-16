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
    logging.basicConfig(
        level=logging.DEBUG if DEBUG else logging.INFO,
        format='[%(asctime)s] %(levelname)s: %(message)s',
        handlers=[logging.StreamHandler()],
        force=True  # Гарантируем, что настройки применятся
    )

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
        """Нормализует лог для поиска паттернов - убирает все переменные данные"""
        if not log_text:
            return ""
        
        # Сначала фильтруем неинформативные строки
        lines = log_text.split('\n')
        filtered_lines = []
        
        for line in lines:
            line_stripped = line.strip()
            if not line_stripped:
                continue
                
            # Исключаем информационные строки, которые не содержат ошибок
            skip_patterns = [
                r' - DEBUG - ',  # DEBUG
                r' - INFO - ',  # INFO
            ]
            
            # Проверяем, нужно ли пропустить эту строку
            should_skip = False
            for pattern in skip_patterns:
                if re.search(pattern, line_stripped):
                    should_skip = True
                    break
            
            if not should_skip:
                filtered_lines.append(line_stripped)
        
        # Объединяем отфильтрованные строки
        normalized = '\n'.join(filtered_lines)
        
        # Применяем существующую нормализацию переменных данных
        # Временные метки (все форматы)
        normalized = re.sub(r'\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}[,.]\d+', '[TIMESTAMP]', normalized)
        normalized = re.sub(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', '[TIMESTAMP]', normalized)
        
        # Пути к файлам (все длинные пути)
        normalized = re.sub(r'/[a-zA-Z0-9/_.-]{20,}', '[PATH]', normalized)
        normalized = re.sub(r'[a-zA-Z0-9/_.-]*build_root[a-zA-Z0-9/_.-]*', '[BUILD_PATH]', normalized)
        
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
        
        # Хеши и идентификаторы
        normalized = re.sub(r'\b[a-f0-9]{16,}\b', '[HASH]', normalized)
        normalized = re.sub(r'\b[A-Fa-f0-9]{8,}\b', '[ID]', normalized)
        
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
        
        # Убираем повторяющиеся пробелы и пустые строки
        normalized = re.sub(r'\s+', ' ', normalized)
        #normalized = re.sub(r'\n\s*\n', '\n', normalized)  # Убираем пустые строки
        
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
    logging.debug(f'Fetching compatibility tests data for last {days_back} days')
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
        
        AND (branch = 'main') and build_type = 'relwithdebinfo'
    ORDER BY run_timestamp DESC
    """

    scan_query = ydb.ScanQuery(query, {})
    it = driver.table_client.scan_query(scan_query)
    while True:
        try:
            result = next(it)
            results.extend(result.result_set.rows)
        except StopIteration:
            break
    
    elapsed = time.time() - start_time
    logging.debug(f'Compatibility tests data retrieved: {len(results)} records (took {elapsed:.2f}s)')
    return results


def parse_test_type_and_versions(test_name):
    """
    Извлекает тип теста и версии из имени теста вида:
    ...[mixed_current_and_25-1-row]
    Возвращает (тип, [версии])
    """
    match = re.search(r'\[(.*?)\]', test_name)
    if not match:
        return ("unknown", "unknown")
    inside = match.group(1)
    parts = inside.split('_')
    test_type = parts[0] if parts else "unknown"
    rest = '_'.join(parts[1:])
    version_pattern = r'(current|\d+-\d+(?:-\d+)?)'
    # _to_ (важен порядок)
    if '_to_' in rest:
        versions_part = rest.split('_to_')
        left_match = re.match(version_pattern, versions_part[0])
        right_match = re.match(version_pattern, versions_part[1])
        left = left_match.group(1) if left_match else 'unknown'
        right = right_match.group(1) if right_match else 'unknown'
        versions_key = f"{left}_to_{right}"
    # _and_ (порядок не важен)
    elif '_and_' in rest:
        versions_part = rest.split('_and_')
        left_match = re.match(version_pattern, versions_part[0])
        right_match = re.match(version_pattern, versions_part[1])
        left = left_match.group(1) if left_match else 'unknown'
        right = right_match.group(1) if right_match else 'unknown'
        versions_key = '_and_'.join(sorted([left, right]))
    else:
        # Просто берём первую подходящую версию после типа
        version_match = re.match(version_pattern, parts[1]) if len(parts) > 1 else None
        version = version_match.group(1) if version_match else 'unknown'
        versions_key = version
    return (test_type, versions_key or 'unknown')


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
    """ЭТАП 4: Группирует по версиям и проверкам"""
    logging.debug("Группируем данные по версиям и типам тестов...")
    
    grouped = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    
    for record in test_data:
        # Формируем полное имя теста
        test_name = f"{record.get('suite_folder', '')}/{record.get('test_name', '')}"
        
        # Парсим тип теста и версии
        test_type, versions_key = parse_test_type_and_versions(test_name)
        
        # Группируем: grouped[версия][тип_теста][имя_теста] = [список запусков]
        grouped[versions_key][test_type][test_name].append({
            'status': record.get('status', ''),
            'timestamp': record.get('run_timestamp'),
            'build_type': record.get('build_type', ''),
            'branch': record.get('branch', ''),
            'job_name': record.get('job_name', ''),
            'commit': record.get('commit', ''),
            'duration': record.get('duration', 0),
            'status_description': record.get('status_description', '')
        })
    
    # Сортируем запуски по времени (новые первыми)
    for version_data in grouped.values():
        for type_data in version_data.values():
            for test_runs in type_data.values():
                test_runs.sort(key=lambda x: x['timestamp'], reverse=True)
    
    logging.debug(f"Сгруппировано по {len(grouped)} версиям:")
    for version, types in grouped.items():
        total_tests = sum(len(tests) for tests in types.values())
        logging.debug(f"  - {version}: {len(types)} типов тестов, {total_tests} тестов")
    
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
                for run in recent_runs:
                    if run.get('status') in ['failure', 'mute'] and run.get('status_description'):
                        error_description = run.get('status_description', '')
                        break
                
                test_info = {
                    'name': test_name,
                    'recent_runs': recent_runs[:5],  # Сохраняем только 5 последних для отчета
                    'fail_rate': fail_count / len(statuses) if statuses else 0,
                    'latest_status': statuses[0] if statuses else 'unknown',
                    'last_time': last_time,
                    'version': version,
                    'type': test_type,
                    'error_description': error_description
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


def extract_meaningful_error_info(text, max_length=1000):
    """Извлекает содержательную информацию об ошибке, сохраняя контекст"""
    if not text:
        return ""
    
    if not isinstance(text, str):
        try:
            text = text.decode('utf-8', errors='replace')
        except Exception:
            text = str(text)
    
    # 1. Ищем ключевые секции в логе
    lines = text.split('\n')
    important_sections = {
        'config_errors': [],      # Ошибки конфигурации (новая категория)
        'startup_errors': [],     # Ошибки запуска (новая категория)
        'errors': [],
        'exceptions': [],
        'failures': [],
        'timeouts': [],
        'assertions': [],
        'context': []
    }
    
    # Ключевые слова для разных типов проблем
    error_patterns = {
        'config_errors': ['unknown field', 'config', 'protobuf', 'json2proto', 'configuration', 'caught exception'],
        'startup_errors': ['start failed', 'kikimr start failed', 'bs_controller', 'daemon failed', 'unexpectedly finished'],
        'errors': ['error', 'failed', 'failure', 'fatal', 'critical'],
        'exceptions': ['exception', 'traceback', 'raise', 'abort'],
        'timeouts': ['timeout', 'timed out', 'deadline', 'expired'],
        'assertions': ['assertion', 'assert', 'expected', 'actual'],
        'context': ['test', 'setup', 'teardown', 'node', 'cluster']
    }
    
    # Специальная обработка для секций Stderr content
    stderr_sections = []
    in_stderr = False
    current_stderr = []
    
    for line in lines:
        line_clean = line.strip()
        if not line_clean:
            continue
            
        # Обнаруживаем начало секции Stderr content
        if 'stderr content:' in line_clean.lower():
            in_stderr = True
            current_stderr = []
            continue
        
        # Обнаруживаем конец секции Stderr (следующая секция или новый daemon)
        if in_stderr and ('daemon failed' in line_clean.lower() or 
                         'stdout file name:' in line_clean.lower() or
                         'process exit_code' in line_clean.lower()):
            if current_stderr:
                stderr_sections.extend(current_stderr)
            in_stderr = False
            current_stderr = []
        
        # Собираем содержимое stderr
        if in_stderr and line_clean and not line_clean.startswith('E   '):
            # Убираем префикс "E   " если есть
            clean_line = line_clean[4:] if line_clean.startswith('E   ') else line_clean
            if clean_line:
                current_stderr.append(clean_line)
    
    # Добавляем последнюю секцию stderr если была
    if current_stderr:
        stderr_sections.extend(current_stderr)
    
    # Классифицируем строки по типам
    all_lines_to_process = lines + stderr_sections  # Обрабатываем и основной лог и stderr секции
    
    for line in all_lines_to_process:
        line_clean = line.strip()
        if not line_clean:
            continue
            
        line_lower = line_clean.lower()
        
        # Специальная обработка для критических ошибок конфигурации
        if ('unknown field' in line_lower and 'enable_batch_updates' in line_lower) or \
           ('caught exception' in line_lower and 'protobuf' in line_lower):
            if len(important_sections['config_errors']) < 8:  # Больше места для критических ошибок
                important_sections['config_errors'].append(line_clean)
            continue
        
        # Проверяем каждый тип ошибки (приоритет: config_errors > startup_errors > остальные)
        categorized = False
        for section, keywords in error_patterns.items():
            if any(keyword in line_lower for keyword in keywords):
                if len(important_sections[section]) < 3:  # Максимум 3 строки каждого типа
                    important_sections[section].append(line_clean)
                categorized = True
                break
        
        # Если строка не попала ни в одну категорию, но содержит важную информацию
        if not categorized and any(word in line_lower for word in ['stderr content', 'process exit_code', 'several daemon errors']):
            if len(important_sections['context']) < 3:
                important_sections['context'].append(line_clean)
    
    # 2. Формируем структурированный вывод с приоритетом
    result_parts = []
    
    # Добавляем ошибки конфигурации (самый высокий приоритет для YDB)
    if important_sections['config_errors']:
        result_parts.append("CONFIG ERRORS:")
        # Убираем дубликаты но сохраняем порядок
        seen = set()
        unique_errors = []
        for error in important_sections['config_errors']:
            # Нормализуем для сравнения (убираем пути и временные метки)
            normalized = re.sub(r'/[a-zA-Z0-9/_.-]{20,}', '[PATH]', error)
            normalized = re.sub(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d+', '[TIMESTAMP]', normalized)
            if normalized not in seen:
                seen.add(normalized)
                unique_errors.append(error)
                if len(unique_errors) >= 3:  # Максимум 3 уникальные ошибки
                    break
        result_parts.extend(unique_errors)
        
        # Если было много одинаковых ошибок, добавляем сводку
        if len(important_sections['config_errors']) > len(unique_errors):
            result_parts.append(f"[... повторяется на {len(important_sections['config_errors']) - len(unique_errors)} узлах ...]")
    
    # Добавляем ошибки запуска
    if important_sections['startup_errors']:
        result_parts.append("STARTUP ERRORS:")
        result_parts.extend(important_sections['startup_errors'])
    
    # Добавляем общие ошибки
    if important_sections['errors']:
        result_parts.append("ERRORS:")
        result_parts.extend(important_sections['errors'])
    
    # Добавляем исключения
    if important_sections['exceptions']:
        result_parts.append("EXCEPTIONS:")
        result_parts.extend(important_sections['exceptions'])
    
    # Добавляем таймауты
    if important_sections['timeouts']:
        result_parts.append("TIMEOUTS:")
        result_parts.extend(important_sections['timeouts'])
    
    # Добавляем проваленные утверждения
    if important_sections['assertions']:
        result_parts.append("ASSERTIONS:")
        result_parts.extend(important_sections['assertions'])
    
    # Добавляем контекст если есть место
    if important_sections['context'] and len('\n'.join(result_parts)) < max_length * 0.7:
        result_parts.append("CONTEXT:")
        result_parts.extend(important_sections['context'][:2])
    
    # 3. Если ничего важного не нашли, берем первые непустые строки
    if not any(important_sections.values()):
        non_empty_lines = [line.strip() for line in lines if line.strip()]
        result_parts = non_empty_lines[:5]
    
    # 4. Объединяем и проверяем длину
    result = '\n'.join(result_parts)
    
    # Очищаем от технических деталей, но сохраняем смысл
    # Для YDB логов особенно важно сохранить названия полей и конфигураций
    result = re.sub(r'/home/runner/\.ya/build/[a-zA-Z0-9/_.-]{30,}', '[BUILD_PATH]', result)  # Длинные build пути
    result = re.sub(r'/[a-zA-Z0-9/_.-]{40,}', '[LONG_PATH]', result)  # Только очень длинные пути
    result = re.sub(r'\b[a-f0-9]{16,}\b', '[HASH]', result)  # Только длинные хеши
    result = re.sub(r':\d{4,5}\b', ':[PORT]', result)  # Порты
    result = re.sub(r'pid: \d+', 'pid: [PID]', result)  # Process IDs
    
    # Финальное ограничение длины с умным обрезанием
    if len(result) > max_length:
        # Обрезаем по границе строк, а не посередине
        lines = result.split('\n')
        truncated_lines = []
        current_length = 0
        
        for line in lines:
            if current_length + len(line) + 1 <= max_length - 20:  # Оставляем место для "[...]"
                truncated_lines.append(line)
                current_length += len(line) + 1
            else:
                break
        
        result = '\n'.join(truncated_lines)
        if len(lines) > len(truncated_lines):
            result += '\n[... остальные строки обрезаны ...]'
    
    return result.strip()


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
    
    prompt = """
Обработай каждый лог ошибки и извлеки ключевую информацию максимально сжато.

Для каждого лога верни объект с ключевой информацией об ошибке:
- Убери повторяющиеся строки
- Убери технические пути и хеши  
- Сохрани суть проблемы
- Максимум 800 символов на лог

Формат ответа (строго JSON):
{
  "log_0": "сжатая ключевая информация об ошибке",
  "log_1": "сжатая ключевая информация об ошибке",
  ...
}

Отвечай ТОЛЬКО JSON, без дополнительных комментариев.
"""
    
    try:
        response = call_single_ai_request(prompt, batch_data)
        if response:
            # Пытаемся распарсить JSON
            cleaned_response = response.strip()
            if cleaned_response.startswith('```json'):
                cleaned_response = cleaned_response[7:-3]
            elif cleaned_response.startswith('```'):
                cleaned_response = cleaned_response[3:-3]
            
            return json.loads(cleaned_response)
    except Exception as e:
        logging.warning(f"AI batch processing failed: {e}")
    
    return None


def smart_error_extraction_with_cache(test_data):
    """Умное извлечение ошибок с кешированием паттернов"""
    logging.debug("=== УМНОЕ ИЗВЛЕЧЕНИЕ ОШИБОК С КЕШИРОВАНИЕМ ===")
    
    mute_records = [r for r in test_data if r.get('status') == 'mute' and r.get('log')]
    if not mute_records:
        return test_data
    
    logging.debug(f"Обрабатываем {len(mute_records)} mute записей")
    
    # Шаг 1: Скачиваем все логи параллельно
    logging.debug("Шаг 1: Скачивание логов...")
    tasks = [(record.get('log'), idx) for idx, record in enumerate(mute_records)]
    
    log_contents = {}
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_task = {executor.submit(fetch_log_content_safe_simple, t): t for t in tasks}
        for future in as_completed(future_to_task):
            idx, content = future.result()
            if content:
                log_contents[idx] = content
    
    # Шаг 2: Проверяем кеш для каждого лога
    logging.debug("Шаг 2: Проверка кеша паттернов...")
    cache_hits = 0
    unknown_logs = []  # Логи, для которых не нашли паттернов
    
    for idx, record in enumerate(mute_records):
        if idx not in log_contents:
            continue
            
        log_content = log_contents[idx]
        
        # Пробуем найти в кеше
        cached_error = error_cache.find_matching_pattern(log_content)
        
        if cached_error:
            # Найден паттерн в кеше
            record['status_description'] = cached_error
            cache_hits += 1
            logging.debug(f"Cache HIT для теста {idx}")
        else:
            # Паттерн не найден, нужна обработка
            unknown_logs.append((idx, record, log_content))
            logging.debug(f"Cache MISS для теста {idx}")
    
    logging.debug(f"Кеш: {cache_hits} попаданий, {len(unknown_logs)} промахов")
    
    # Шаг 3: Обрабатываем неизвестные логи
    if unknown_logs:
        logging.debug("Шаг 3: Обработка неизвестных логов...")
        
        # Сначала базовая обработка
        basic_processed = []
        ai_needed = []
        
        for idx, record, log_content in unknown_logs:
            # Hybrid подход: сначала базовая обработка
            basic_result = extract_meaningful_error_info(log_content, max_length=1500)
            
            # Определяем, нужен ли AI
            if needs_ai_processing(basic_result):
                ai_needed.append((idx, record, log_content, basic_result))
            else:
                # Базовой обработки достаточно
                record['status_description'] = basic_result
                error_cache.add_pattern(log_content, basic_result)
                basic_processed.append(idx)
        
        logging.debug(f"Базовая обработка: {len(basic_processed)}, AI нужен: {len(ai_needed)}")
        
        # Шаг 4: Обрабатываем через AI батчами
        if ai_needed:
            logging.debug("Шаг 4: AI обработка батчами...")
            
            batch_size = 6  # Оптимальный размер батча
            batches = [ai_needed[i:i+batch_size] for i in range(0, len(ai_needed), batch_size)]
            
            api_calls_made = 0
            
            for batch in batches:
                # Подготавливаем данные для батча
                batch_data = {}
                for i, (idx, record, log_content, basic_result) in enumerate(batch):
                    batch_data[f"log_{i}"] = {
                        'original_log': log_content[:8000],  # Ограничиваем размер
                        'basic_processing': basic_result
                    }
                
                # AI запрос для батча
                ai_results = process_error_batch_with_ai(batch_data)
                api_calls_made += 1
                
                if ai_results:
                    # Применяем результаты AI
                    for i, (idx, record, log_content, _) in enumerate(batch):
                        ai_result = ai_results.get(f"log_{i}")
                        if ai_result:
                            record['status_description'] = ai_result
                            error_cache.add_pattern(log_content, ai_result)
                        else:
                            # Fallback на базовую обработку
                            record['status_description'] = basic_result
                            error_cache.add_pattern(log_content, basic_result)
                else:
                    # AI не сработал, используем базовую обработку
                    for idx, record, log_content, basic_result in batch:
                        record['status_description'] = basic_result
                        error_cache.add_pattern(log_content, basic_result)
            
            logging.debug(f"Сделано {api_calls_made} AI запросов для {len(ai_needed)} логов")
    
    # Сохраняем кеш
    error_cache.save_cache()
    
    # Статистика
    total_processed = len(mute_records)
    logging.info(f"=== СТАТИСТИКА ОБРАБОТКИ ОШИБОК ===")
    logging.info(f"Всего записей: {total_processed}")
    logging.info(f"Кеш попадания: {cache_hits} ({cache_hits/total_processed*100:.1f}%)")
    logging.info(f"Новых паттернов: {error_cache.new_patterns_count}")
    logging.info(f"Всего паттернов в кеше: {len(error_cache.patterns)}")
    
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


def generate_compatibility_report():
    setup_logging()
    logging.debug("Starting compatibility tests AI report generation")
    
    if "CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS" not in os.environ:
        logging.debug("Error: Env variable CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS is missing")
        return 1
    else:
        os.environ["YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"] = os.environ[
            "CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"
        ]

    with ydb.Driver(
        endpoint=DATABASE_ENDPOINT,
        database=DATABASE_PATH,
        credentials=ydb.credentials_from_env_variables()
    ) as driver:
        driver.wait(timeout=10, fail_fast=True)
        
        # ===== ЭТАП 1: ПОЛУЧИТЬ ДАННЫЕ ТЕСТОВ ИЗ БД =====
        logging.debug("=== ЭТАП 1: ПОЛУЧЕНИЕ ДАННЫХ ИЗ БД ===")
        test_data = get_compatibility_tests_data(driver, days_back=7)
        
        if not test_data:
            logging.debug("No compatibility test data found")
            return 0
        
        logging.debug(f"Получено {len(test_data)} записей из БД")
        if DEBUG: save_json(test_data, 'analytics_debug_1_raw_data.json')
        
        # ===== ЭТАП 2: ЗАМЕНИТЬ STATUS_DESCRIPTION У MUTE РЕЗУЛЬТАТОВ ИЗ LOG =====
        logging.debug("=== ЭТАП 2: ОБОГАЩЕНИЕ MUTE ЗАПИСЕЙ ЛОГАМИ ===")
        enriched_data = enrich_mute_records_with_logs(test_data)
        
        logging.debug(f"Обогащение завершено")
        if DEBUG: save_json(enriched_data, 'analytics_debug_2_enriched_data.json')
        
        # ===== ЭТАП 3: ИСКЛЮЧИТЬ ТЕСТЫ БЕЗ STATUS_DESCRIPTION ИЗ LOG =====
        logging.debug("=== ЭТАП 3: ФИЛЬТРАЦИЯ ЗАПИСЕЙ ===")
        filtered_data = filter_records_with_status_description(enriched_data)
        
        logging.debug(f"После фильтрации осталось {len(filtered_data)} записей")
        if DEBUG: save_json(filtered_data, 'analytics_debug_3_filtered_data.json')
        
        # ===== ЭТАП 4: СГРУППИРОВАТЬ ПО ВЕРСИЯМ И ПРОВЕРКАМ =====
        logging.debug("=== ЭТАП 4: ГРУППИРОВКА ПО ВЕРСИЯМ И ТИПАМ ===")
        grouped_data = group_by_versions_and_types(filtered_data)
        
        logging.debug(f"Сгруппировано по {len(grouped_data)} версиям")
        if DEBUG: save_json(grouped_data, 'analytics_debug_4_grouped_data.json')
        
        # ===== ЭТАП 5: СОБРАТЬ ГРУППЫ ДЛЯ ПЕРЕДАЧИ В AI =====
        logging.debug("=== ЭТАП 5: ПОДГОТОВКА ДАННЫХ ДЛЯ AI ===")
        ai_ready_data = prepare_data_for_ai_analysis(grouped_data)
        
        logging.debug(f"Подготовлены данные для AI анализа")
        if DEBUG: save_json(ai_ready_data, 'analytics_debug_5_ai_ready_data.json')
        
        # ===== ГЕНЕРАЦИЯ ОТЧЕТОВ =====
        logging.debug("=== ГЕНЕРАЦИЯ ОТЧЕТОВ ===")
        
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
        
        logging.debug(f"Generated {len(generated_reports)} version reports in: {reports_dir}")
        logging.debug(f"Index report: {index_path}")
        
        # Выводим сводку
        print(f"\n{'='*60}")
        print(f"ОТЧЕТЫ ПО СОВМЕСТИМОСТИ СГЕНЕРИРОВАНЫ")
        print(f"{'='*60}")
        print(f"Папка с отчетами: {reports_dir}")
        print(f"Индексный файл: {index_path}")
        print(f"\nСгенерировано отчетов по версиям: {len(generated_reports)}")
        for version, filename, tests_count in generated_reports:
            print(f"  - {version}: {filename} ({tests_count} тестов)")
        print(f"{'='*60}")
        
        return 0


def generate_version_report(version, version_data, ai_ready_data, output_dir):
    """Генерирует отчет для конкретной версии по четкой схеме:
    1) Подготавливаем данные
    2) Кластеризация ошибок через AI
    3) Общий отчет через AI
    """
    logging.debug(f"=== Generating report for version: {version} ===")
    
    # ШАГ 1: ПОДГОТАВЛИВАЕМ ДАННЫЕ
    logging.debug("Step 1: Preparing data...")
    
    # Собираем нестабильные тесты за 24 часа для этой версии
    unstable_tests_24h = []
    not_run_24h = []
    now = datetime.utcnow()
    
    for test_type, type_data in version_data.items():
        for test in type_data.get('all_tests', []):
            # Нестабильные тесты за 24 часа
            if is_unstable_last_24h(test['recent_runs'], now):
                unstable_tests_24h.append(test)
            
            # Тесты, не запускавшиеся более суток
            if test['last_time'] and (now - test['last_time']) > timedelta(days=1):
                not_run_24h.append(test)
    
    version_analysis = {
        'version': version,
        'summary_stats': {
            'total_tests': ai_ready_data['summary_stats']['by_version'].get(version, {}).get('total', 0),
            'passed': ai_ready_data['summary_stats']['by_version'].get(version, {}).get('passed', 0),
            'failure': ai_ready_data['summary_stats']['by_version'].get(version, {}).get('failure', 0),
            'mute': ai_ready_data['summary_stats']['by_version'].get(version, {}).get('mute', 0)
        },
        'by_type': version_data,
        'not_run_24h': not_run_24h,
        'unstable_tests_24h': unstable_tests_24h
    }
    
    # Данные для кластеризации ошибок
    error_clustering_data = [
        {'test': t['name'], 'error': t['error_description']} 
        for t in unstable_tests_24h if t.get('error_description')
    ]
    
    logging.debug(f"Data prepared: {len(unstable_tests_24h)} unstable tests, {len(error_clustering_data)} with errors")
    
    # ШАГ 2: КЛАСТЕРИЗАЦИЯ ОШИБОК ЧЕРЕЗ AI
    logging.debug("Step 2: AI Error Clustering...")
    error_clustering_result = None
    
    if error_clustering_data:
        clustering_prompt = """
Проанализируй список ошибок тестов и сгруппируй их по смыслу. 
Для каждого кластера ошибок укажи:
1. Краткое описание причины
2. Ключевую выдержку из текста ошибки (самую значимую часть)
3. Список тестов, где встречается эта ошибка

Игнорируй различия в путях, временных метках, номерах портов, хешах коммитов и других переменных данных. 
Сосредоточься на сути проблемы.

Формат ответа:
## Кластер N: [Краткое описание]
**Ключевая ошибка:** `[значимая выдержка из лога]`
**Затронутые тесты:**
- test_name_1
- test_name_2
"""
        
        error_clustering_result = call_single_ai_request(clustering_prompt, error_clustering_data)
        if error_clustering_result:
            logging.debug(f"Error clustering completed: {len(error_clustering_result)} chars")
        else:
            logging.debug("Error clustering failed")
    else:
        logging.debug("No errors to cluster")
    
    # ШАГ 3: ОБЩИЙ ОТЧЕТ ЧЕРЕЗ AI
    logging.debug("Step 3: AI General Report...")
    
    main_prompt = f"""
Ты - эксперт по анализу результатов тестирования YDB. 
Проанализируй данные о compatibility тестах для версии {version} и создай подробный отчет на русском языке.

Твоя задача:
1. Проанализировать статистику тестов для версии {version}
2. Выявить тесты, которые не запускались более суток
3. Выявить нестабильные тесты за последние 24 часа
4. Проанализировать типы тестов (mixed, upgrade, etc.)
5. Дать рекомендации по исправлению проблем

Структурируй отчет с заголовками и markdown-таблицами для удобства чтения.
Начни отчет с заголовка: # Отчет по совместимости для версии {version}
"""
    
    ai_report = call_single_ai_request(main_prompt, version_analysis)
    if ai_report:
        logging.debug(f"Main AI report completed: {len(ai_report)} chars")
    else:
        logging.debug("Main AI report failed")
    
    # ГЕНЕРИРУЕМ ФИНАЛЬНЫЙ ОТЧЕТ
    logging.debug("Step 4: Generating final report...")
    report = generate_version_mock_report(version_analysis, error_clustering_result, ai_report)
    
    # СОХРАНЯЕМ ОТЧЕТ
    report_filename = f"compatibility_report_{version.replace('_', '-')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
    report_path = os.path.join(output_dir, report_filename)
    
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(report)
    
    logging.debug(f"=== Version report saved: {report_path} ===")
    return report_path


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
        
        report += f"| {test_type} | {total} | {failed} | {flaky} | {new_failures} | {skipped} |\n"
    
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
            err = (test['error_description'] or '').replace('\n', ' ')[:100]
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


if __name__ == "__main__":
    exit(generate_compatibility_report()) 