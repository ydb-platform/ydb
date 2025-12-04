#!/usr/bin/env python3
"""
Скрипт для проверки синхронизации между muted_ya.txt и таблицей tests_monitor.
Показывает рассинхронизацию: какие тесты есть в muted_ya.txt, но не замьючены в YDB,
и наоборот - какие тесты замьючены в YDB, но отсутствуют в muted_ya.txt.
"""

import argparse
import datetime
import sys
import os
from pathlib import Path
from collections import defaultdict

# Check for required dependencies
try:
    import ydb
except ImportError:
    print("❌ Error: Module 'ydb' is not installed")
    print("   Please install dependencies:")
    print("   pip install ydb[yc]")
    sys.exit(1)

# Add analytics directory to path for ydb_wrapper import
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'analytics'))
try:
    from ydb_wrapper import YDBWrapper
except ImportError as e:
    print(f"❌ Error: Failed to import YDBWrapper: {e}")
    print("   Make sure you're on the correct branch with ydb_wrapper.py")
    sys.exit(1)

# Add parent directory to import YaMuteCheck
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
try:
    from transform_ya_junit import YaMuteCheck
except ImportError as e:
    print(f"❌ Error: Failed to import YaMuteCheck: {e}")
    sys.exit(1)


def load_muted_ya_tests(muted_ya_path):
    """Загружает список тестов из muted_ya.txt"""
    muted_tests = set()  # full_name в формате "suite_folder/test_name"
    
    if not os.path.exists(muted_ya_path):
        print(f"⚠️  Файл muted_ya.txt не найден: {muted_ya_path}")
        return muted_tests
    
    try:
        mute_check = YaMuteCheck()
        mute_check.load(muted_ya_path)
        
        # Извлекаем все паттерны из muted_ya.txt
        with open(muted_ya_path, 'r') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                try:
                    suite_folder, test_name = line.split(" ", maxsplit=1)
                    # Нормализуем формат: suite_folder/test_name (как в YDB)
                    full_name = f"{suite_folder}/{test_name}"
                    muted_tests.add(full_name)
                except ValueError:
                    print(f"⚠️  Пропущена некорректная строка {line_num}: {line}")
                    continue
        
        print(f"✅ Загружено {len(muted_tests)} тестов из {muted_ya_path}")
        return muted_tests
        
    except Exception as e:
        print(f"❌ Ошибка при загрузке muted_ya.txt: {e}")
        import traceback
        traceback.print_exc()
        return muted_tests, muted_tests_raw


def get_muted_tests_from_ydb(branch, build_type='relwithdebinfo', date_window_days=1):
    """Получает список замьюченных тестов из tests_monitor за указанный период"""
    muted_tests = {}  # full_name -> is_muted
    
    with YDBWrapper() as ydb_wrapper:
        if not ydb_wrapper.check_credentials():
            print("❌ Не удалось проверить credentials")
            return muted_tests
        
        tests_monitor_table = ydb_wrapper.get_table_path("tests_monitor", database="main")
        
        # Получаем тесты за последние N дней (по умолчанию сегодня)
        # Берем последнюю запись для каждого теста (по date_window)
        query = f'''
        SELECT 
            suite_folder,
            test_name,
            suite_folder || '/' || test_name as full_name,
            is_muted,
            date_window
        FROM `{tests_monitor_table}`
        WHERE date_window >= CurrentUtcDate() - {date_window_days}*Interval("P1D")
            AND branch = '{branch}'
            AND build_type = '{build_type}'
        ORDER BY suite_folder, test_name, date_window DESC
        '''
        
        try:
            results = ydb_wrapper.execute_scan_query(query, query_name=f"get_muted_from_ydb_{branch}")
            
            # Для каждого теста берем только последнюю запись (первую после сортировки по date_window DESC)
            seen_tests = set()
            for row in results:
                full_name = row.get('full_name')
                is_muted = row.get('is_muted', 0)
                # Конвертируем bytes в строку если нужно
                if isinstance(full_name, bytes):
                    full_name = full_name.decode('utf-8')
                if full_name and full_name not in seen_tests:
                    muted_tests[full_name] = is_muted
                    seen_tests.add(full_name)
            
            print(f"✅ Получено {len(muted_tests)} уникальных тестов из tests_monitor")
            return muted_tests
            
        except Exception as e:
            print(f"❌ Ошибка при выполнении запроса: {e}")
            import traceback
            traceback.print_exc()
            return muted_tests


def check_sync(muted_ya_tests, ydb_muted_tests, branch):
    """Проверяет синхронизацию между muted_ya.txt и YDB"""
    print(f"\n{'='*80}")
    print(f"📊 АНАЛИЗ СИНХРОНИЗАЦИИ")
    print(f"{'='*80}")
    
    # Тесты, которые есть в muted_ya.txt
    muted_ya_set = set(muted_ya_tests)
    
    # Конвертируем все ключи из ydb_muted_tests в строки (на случай bytes)
    def to_str(value):
        """Конвертирует bytes в строку если нужно"""
        if isinstance(value, bytes):
            return value.decode('utf-8')
        return str(value)
    
    ydb_muted_tests_str = {to_str(name): is_muted for name, is_muted in ydb_muted_tests.items()}
    
    # Тесты, которые замьючены в YDB (is_muted=1)
    ydb_muted_set = {name for name, is_muted in ydb_muted_tests_str.items() if is_muted == 1}
    
    # Тесты, которые НЕ замьючены в YDB (is_muted=0)
    ydb_unmuted_set = {name for name, is_muted in ydb_muted_tests_str.items() if is_muted == 0}
    
    # 1. Тесты в muted_ya.txt, но НЕ замьючены в YDB
    in_muted_ya_not_in_ydb = muted_ya_set - ydb_muted_set
    
    # 2. Тесты замьючены в YDB, но НЕТ в muted_ya.txt
    in_ydb_not_in_muted_ya = ydb_muted_set - muted_ya_set
    
    # 3. Тесты в обоих местах (синхронизированы)
    synced = muted_ya_set & ydb_muted_set
    
    # 4. Тесты в muted_ya.txt, но есть в YDB с is_muted=0 (явная рассинхронизация)
    explicitly_desynced = muted_ya_set & ydb_unmuted_set
    
    print(f"\n📈 СТАТИСТИКА:")
    print(f"   Всего в muted_ya.txt: {len(muted_ya_set)}")
    print(f"   Замьючено в YDB (is_muted=1): {len(ydb_muted_set)}")
    print(f"   НЕ замьючено в YDB (is_muted=0): {len(ydb_unmuted_set)}")
    print(f"   Синхронизировано (есть в обоих): {len(synced)}")
    print(f"   Рассинхронизировано: {len(in_muted_ya_not_in_ydb) + len(in_ydb_not_in_muted_ya)}")
    
    print(f"\n{'='*80}")
    print(f"🔴 ПРОБЛЕМЫ СИНХРОНИЗАЦИИ")
    print(f"{'='*80}")
    
    # Проблема 1: В muted_ya.txt, но не замьючено в YDB
    if explicitly_desynced:
        print(f"\n❌ ПРОБЛЕМА 1: Тесты в muted_ya.txt, но is_muted=0 в YDB ({len(explicitly_desynced)} тестов)")
        print(f"   Это означает, что тесты должны быть замьючены, но в YDB они не замьючены")
        print(f"   Возможные причины:")
        print(f"   - get_muted_tests.py не обновил is_muted (тест не попал в get_all_tests)")
        print(f"   - Тест не запускался за последние 90 дней")
        print(f"   - Тест отсутствует в testowners")
        print(f"\n   Примеры (первые 10):")
        for i, test in enumerate(sorted(explicitly_desynced)[:10], 1):
            print(f"   {i}. {test}")
        if len(explicitly_desynced) > 10:
            print(f"   ... и еще {len(explicitly_desynced) - 10} тестов")
    
    # Проблема 2: Замьючено в YDB, но нет в muted_ya.txt
    if in_ydb_not_in_muted_ya:
        print(f"\n❌ ПРОБЛЕМА 2: Тесты замьючены в YDB (is_muted=1), но НЕТ в muted_ya.txt ({len(in_ydb_not_in_muted_ya)} тестов)")
        print(f"   Это означает, что тесты были удалены из muted_ya.txt, но is_muted не обновился")
        print(f"   Возможные причины:")
        print(f"   - Тест был удален из muted_ya.txt, но не запускался недавно")
        print(f"   - get_muted_tests.py не обновил is_muted (тест не попал в get_all_tests)")
        print(f"   - Тест не запускался за последные 90 дней для ветки {branch}")
        print(f"\n   Примеры (первые 10):")
        for i, test in enumerate(sorted(in_ydb_not_in_muted_ya)[:10], 1):
            print(f"   {i}. {test}")
        if len(in_ydb_not_in_muted_ya) > 10:
            print(f"   ... и еще {len(in_ydb_not_in_muted_ya) - 10} тестов")
    
    # Тесты в muted_ya.txt, но нет данных в YDB вообще
    ydb_all_tests = set(ydb_muted_tests_str.keys())
    in_muted_ya_no_ydb_data = muted_ya_set - ydb_all_tests
    if in_muted_ya_no_ydb_data:
        print(f"\n⚠️  ПРОБЛЕМА 3: Тесты в muted_ya.txt, но НЕТ данных в YDB за период ({len(in_muted_ya_no_ydb_data)} тестов)")
        print(f"   Это означает, что тесты не запускались за указанный период")
        print(f"   Примеры (первые 10):")
        for i, test in enumerate(sorted(in_muted_ya_no_ydb_data)[:10], 1):
            print(f"   {i}. {test}")
        if len(in_muted_ya_no_ydb_data) > 10:
            print(f"   ... и еще {len(in_muted_ya_no_ydb_data) - 10} тестов")
    
    # Проверяем формат full_name - возможно проблема в формате
    if len(synced) == 0 and (len(muted_ya_set) > 0 or len(ydb_muted_set) > 0):
        print(f"\n⚠️  ВНИМАНИЕ: Синхронизировано 0 тестов!")
        print(f"   Это может означать проблему с форматом full_name")
        print(f"   Проверяем формат...")
        
        # Показываем примеры форматов
        if muted_ya_set:
            example_muted_ya = list(muted_ya_set)[0]
            print(f"\n   Пример из muted_ya.txt: {example_muted_ya}")
        
        if ydb_muted_set:
            example_ydb = list(ydb_muted_set)[0]
            print(f"   Пример из YDB: {example_ydb}")
        
        # Пытаемся найти похожие тесты (fuzzy matching)
        print(f"\n   🔍 Поиск похожих тестов (fuzzy matching)...")
        found_similar = 0
        
        def to_str(value):
            """Конвертирует bytes в строку если нужно"""
            if isinstance(value, bytes):
                return value.decode('utf-8')
            return str(value)
        
        for muted_ya_test in list(muted_ya_set)[:20]:  # Проверяем первые 20
            # Извлекаем suite_folder и test_name из muted_ya
            parts = muted_ya_test.split('/', 1)
            if len(parts) == 2:
                suite_from_file, test_from_file = parts
                # Ищем в YDB по suite_folder и test_name отдельно
                for ydb_test in ydb_muted_set:
                    ydb_test_str = to_str(ydb_test)
                    if suite_from_file in ydb_test_str and test_from_file in ydb_test_str:
                        print(f"      Найдено похожее:")
                        print(f"        muted_ya.txt: {muted_ya_test}")
                        print(f"        YDB:          {ydb_test_str}")
                        found_similar += 1
                        if found_similar >= 5:  # Показываем только первые 5
                            break
                if found_similar >= 5:
                    break
        
        if found_similar == 0:
            print(f"      Похожих тестов не найдено")
            print(f"      Возможно, тесты из muted_ya.txt не запускались для ветки {branch}")
    
    if not explicitly_desynced and not in_ydb_not_in_muted_ya and len(synced) > 0:
        print(f"\n✅ СИНХРОНИЗАЦИЯ ИДЕАЛЬНАЯ!")
        print(f"   Все тесты из muted_ya.txt замьючены в YDB")
        print(f"   Все замьюченные тесты в YDB есть в muted_ya.txt")
    
    # Сохраняем результаты в файлы
    output_dir = "mute_sync_check"
    os.makedirs(output_dir, exist_ok=True)
    
    if explicitly_desynced:
        file_path = os.path.join(output_dir, f"in_muted_ya_not_muted_in_ydb_{branch}.txt")
        with open(file_path, 'w') as f:
            for test in sorted(explicitly_desynced):
                f.write(f"{test}\n")
        print(f"\n💾 Список сохранен в: {file_path}")
    
    if in_ydb_not_in_muted_ya:
        file_path = os.path.join(output_dir, f"muted_in_ydb_not_in_muted_ya_{branch}.txt")
        with open(file_path, 'w') as f:
            for test in sorted(in_ydb_not_in_muted_ya):
                f.write(f"{test}\n")
        print(f"💾 Список сохранен в: {file_path}")
    
    return {
        'total_muted_ya': len(muted_ya_set),
        'total_ydb_muted': len(ydb_muted_set),
        'synced': len(synced),
        'in_muted_ya_not_muted': len(explicitly_desynced),
        'muted_in_ydb_not_in_file': len(in_ydb_not_in_muted_ya),
        'in_muted_ya_no_ydb_data': len(in_muted_ya_no_ydb_data)
    }


def main():
    parser = argparse.ArgumentParser(
        description="Проверка синхронизации между muted_ya.txt и таблицей tests_monitor"
    )
    parser.add_argument('--branch', required=True, 
                      help='Ветка (например: stable-25-3)')
    parser.add_argument('--build_type', default='relwithdebinfo', 
                      help='Тип сборки (default: relwithdebinfo)')
    parser.add_argument('--muted_ya_file', 
                      help='Путь к файлу muted_ya.txt (по умолчанию: .github/config/muted_ya.txt)')
    parser.add_argument('--days', type=int, default=1,
                      help='Количество дней для проверки в YDB (default: 1, т.е. сегодня)')
    
    args = parser.parse_args()
    
    print(f"\n{'#'*80}")
    print(f"🔍 ПРОВЕРКА СИНХРОНИЗАЦИИ muted_ya.txt ↔ tests_monitor")
    print(f"{'#'*80}")
    print(f"Branch: {args.branch}")
    print(f"Build type: {args.build_type}")
    print(f"Period: последние {args.days} дней")
    print(f"{'#'*80}\n")
    
    # Определяем путь к muted_ya.txt
    if args.muted_ya_file:
        muted_ya_path = args.muted_ya_file
    else:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        repo_path = os.path.join(script_dir, '../../../')
        muted_ya_path = os.path.join(repo_path, '.github/config/muted_ya.txt')
    
    print(f"📄 Используется muted_ya.txt: {muted_ya_path}")
    
    # Загружаем тесты из muted_ya.txt
    muted_ya_tests = load_muted_ya_tests(muted_ya_path)
    
    # Получаем тесты из YDB
    ydb_muted_tests = get_muted_tests_from_ydb(args.branch, args.build_type, args.days)
    
    # Проверяем синхронизацию
    stats = check_sync(muted_ya_tests, ydb_muted_tests, args.branch)
    
    # Итоговая сводка с рекомендациями
    print(f"\n{'#'*80}")
    print(f"📊 ИТОГОВАЯ СВОДКА")
    print(f"{'#'*80}")
    print(f"Всего в muted_ya.txt: {stats['total_muted_ya']}")
    print(f"Замьючено в YDB: {stats['total_ydb_muted']}")
    print(f"Синхронизировано: {stats['synced']}")
    print(f"Рассинхронизировано:")
    print(f"  - В muted_ya.txt, но не замьючено в YDB: {stats['in_muted_ya_not_muted']}")
    print(f"  - Замьючено в YDB, но нет в muted_ya.txt: {stats['muted_in_ydb_not_in_file']}")
    print(f"  - В muted_ya.txt, но нет данных в YDB: {stats['in_muted_ya_no_ydb_data']}")
    
    print(f"\n💡 РЕКОМЕНДАЦИИ ПО ИСПРАВЛЕНИЮ:")
    
    if stats['synced'] == 0 and stats['total_muted_ya'] > 0:
        print(f"\n  ⚠️  КРИТИЧЕСКАЯ ПРОБЛЕМА: Синхронизировано 0 тестов!")
        print(f"     Возможные причины:")
        print(f"     1. Тесты из muted_ya.txt не запускались для ветки {branch} за период")
        print(f"     2. Несоответствие формата full_name между muted_ya.txt и YDB")
        print(f"     3. Тесты запускались для другой ветки")
        print(f"\n     Действия:")
        print(f"     1. Проверить, для какой ветки собирались данные в tests_monitor")
        print(f"     2. Увеличить период проверки: --days 7 или --days 30")
        print(f"     3. Проверить формат full_name в примерах выше")
    
    if stats['in_muted_ya_not_muted'] > 0:
        print(f"\n  📝 Для {stats['in_muted_ya_not_muted']} тестов в muted_ya.txt, но не замьюченных в YDB:")
        print(f"     - Запустить: get_muted_tests.py upload_muted_tests --branch {branch}")
        print(f"     - Проверить, что тесты попадают в get_all_tests (есть в testowners, запускались за 90 дней)")
    
    if stats['muted_in_ydb_not_in_file'] > 0:
        print(f"\n  📝 Для {stats['muted_in_ydb_not_in_file']} тестов замьюченных в YDB, но отсутствующих в muted_ya.txt:")
        print(f"     - Это нормально, если тесты были удалены из muted_ya.txt")
        print(f"     - is_muted обновится при следующем запуске get_muted_tests.py (если тест запустится)")
        print(f"     - Или добавить тесты обратно в muted_ya.txt, если они должны быть замьючены")
    
    if stats['in_muted_ya_no_ydb_data'] > 0:
        print(f"\n  📝 Для {stats['in_muted_ya_no_ydb_data']} тестов в muted_ya.txt без данных в YDB:")
        print(f"     - Тесты не запускались за указанный период")
        print(f"     - Увеличить период: --days 7 или --days 30")
        print(f"     - Проверить, запускались ли тесты для ветки {branch} вообще")
    
    print(f"{'#'*80}\n")


if __name__ == "__main__":
    main()

