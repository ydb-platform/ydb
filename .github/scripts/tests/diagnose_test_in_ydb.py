#!/usr/bin/env python3
"""
Диагностический скрипт для проверки наличия теста в YDB для конкретной ветки.
Помогает понять, почему тест отсутствует в результатах create_new_muted_ya.py

Проверяет все этапы обработки:
1. test_results - сырые данные о запусках тестов
2. flaky_tests_window - агрегированная история тестов
3. all_tests_with_owner_and_mute - тесты с владельцами и mute статусом
4. tests_monitor - финальная таблица мониторинга
5. mute_check фильтр - проверка по muted_ya.txt
6. Симуляция запроса execute_query - точная копия запроса из create_new_muted_ya.py
7. Агрегация данных - проверка логики aggregate_test_data
"""

import argparse
import datetime
import sys
import os
from pathlib import Path

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


def check_test_in_tests_monitor(test_name, suite_folder, branch, build_type='relwithdebinfo', days_back=30):
    """Проверяет наличие теста в таблице tests_monitor"""
    print(f"\n{'='*80}")
    print(f"🔍 Проверка в таблице tests_monitor")
    print(f"{'='*80}")
    print(f"Test: {suite_folder} {test_name}")
    print(f"Branch: {branch}")
    print(f"Build type: {build_type}")
    print(f"Период: последние {days_back} дней")
    
    latest_is_muted = None
    
    with YDBWrapper() as ydb_wrapper:
        if not ydb_wrapper.check_credentials():
            print("❌ Не удалось проверить credentials")
            return
        
        tests_monitor_table = ydb_wrapper.get_table_path("tests_monitor", database="main")
        
        # Проверяем за последние N дней
        query = f'''
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
        FROM `{tests_monitor_table}`
        WHERE date_window >= CurrentUtcDate() - {days_back}*Interval("P1D")
            AND branch = '{branch}' 
            AND build_type = '{build_type}'
            AND suite_folder = '{suite_folder}'
            AND test_name = '{test_name}'
        ORDER BY date_window DESC
        LIMIT 50
        '''
        
        print(f"\n📊 SQL запрос:\n{query}\n")
        
        try:
            results = ydb_wrapper.execute_scan_query(query, query_name=f"diagnose_test_{branch}")
            
            if results:
                print(f"✅ Найдено {len(results)} записей в tests_monitor:")
                print(f"\n{'Дата':<12} {'Pass':<6} {'Fail':<6} {'Mute':<6} {'Skip':<6} {'State':<20} {'Muted':<8}")
                print("-" * 80)
                for row in results[:10]:  # Показываем первые 10
                    date = row.get('date_window', 'N/A')
                    if isinstance(date, int):
                        base_date = datetime.date(1970, 1, 1)
                        date = (base_date + datetime.timedelta(days=date)).strftime('%Y-%m-%d')
                    print(f"{date:<12} {row.get('pass_count', 0):<6} {row.get('fail_count', 0):<6} "
                          f"{row.get('mute_count', 0):<6} {row.get('skip_count', 0):<6} "
                          f"{row.get('state', 'N/A'):<20} {row.get('is_muted', 0):<8}")
                
                if len(results) > 10:
                    print(f"... и еще {len(results) - 10} записей")
                
                # Сохраняем is_muted из самой последней записи (первой в отсортированном списке)
                latest_is_muted = results[0].get('is_muted', 0)
                
                # Проверяем последние 7 дней
                today = datetime.date.today()
                seven_days_ago = today - datetime.timedelta(days=7)
                recent_results = []
                for row in results:
                    date = row.get('date_window')
                    if isinstance(date, int):
                        base_date = datetime.date(1970, 1, 1)
                        date = base_date + datetime.timedelta(days=date)
                    if isinstance(date, datetime.date) and date >= seven_days_ago:
                        recent_results.append(row)
                
                if recent_results:
                    print(f"\n✅ Найдено {len(recent_results)} записей за последние 7 дней")
                else:
                    print(f"\n⚠️  НЕТ записей за последние 7 дней (это объясняет отсутствие в create_new_muted_ya.py)")
                    if results:
                        date = results[0].get('date_window')
                        if isinstance(date, int):
                            base_date = datetime.date(1970, 1, 1)
                            date = (base_date + datetime.timedelta(days=date)).strftime('%Y-%m-%d')
                        print(f"   Последняя запись: {date}")
            else:
                print(f"❌ Записей не найдено в tests_monitor за последние {days_back} дней")
                latest_is_muted = None
            
            return latest_is_muted
                
        except Exception as e:
            print(f"❌ Ошибка при выполнении запроса: {e}")
            import traceback
            traceback.print_exc()
            return None


def check_test_in_flaky_tests_window(test_name, suite_folder, branch, build_type='relwithdebinfo', days_back=30):
    """Проверяет наличие теста в таблице flaky_tests_window"""
    print(f"\n{'='*80}")
    print(f"🔍 Проверка в таблице flaky_tests_window")
    print(f"{'='*80}")
    
    with YDBWrapper() as ydb_wrapper:
        if not ydb_wrapper.check_credentials():
            print("❌ Не удалось проверить credentials")
            return
        
        flaky_tests_table = ydb_wrapper.get_table_path("flaky_tests_window", database="main")
        
        query = f'''
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
            history,
            history_class
        FROM `{flaky_tests_table}`
        WHERE date_window >= CurrentUtcDate() - {days_back}*Interval("P1D")
            AND branch = '{branch}' 
            AND build_type = '{build_type}'
            AND suite_folder = '{suite_folder}'
            AND test_name = '{test_name}'
        ORDER BY date_window DESC
        LIMIT 20
        '''
        
        try:
            results = ydb_wrapper.execute_scan_query(query, query_name=f"diagnose_flaky_{branch}")
            
            if results:
                print(f"✅ Найдено {len(results)} записей в flaky_tests_window")
                print(f"\n{'Дата':<12} {'Pass':<6} {'Fail':<6} {'Mute':<6} {'History':<30}")
                print("-" * 80)
                for row in results[:10]:
                    date = row.get('date_window', 'N/A')
                    if isinstance(date, int):
                        base_date = datetime.date(1970, 1, 1)
                        date = (base_date + datetime.timedelta(days=date)).strftime('%Y-%m-%d')
                    history = row.get('history', 'N/A')
                    if isinstance(history, bytes):
                        history = history.decode('utf-8')
                    history = (history[:27] + '...') if len(str(history)) > 30 else history
                    print(f"{date:<12} {row.get('pass_count', 0):<6} {row.get('fail_count', 0):<6} "
                          f"{row.get('mute_count', 0):<6} {history:<30}")
            else:
                print(f"❌ Записей не найдено в flaky_tests_window за последние {days_back} дней")
                
        except Exception as e:
            print(f"❌ Ошибка при выполнении запроса: {e}")


def check_test_in_test_results(test_name, suite_folder, branch, build_type='relwithdebinfo', days_back=30):
    """Проверяет наличие теста в таблице test_results (сырые данные)"""
    print(f"\n{'='*80}")
    print(f"🔍 Проверка в таблице test_results (сырые данные)")
    print(f"{'='*80}")
    
    with YDBWrapper() as ydb_wrapper:
        if not ydb_wrapper.check_credentials():
            print("❌ Не удалось проверить credentials")
            return
        
        test_runs_table = ydb_wrapper.get_table_path("test_results", database="main")
        
        query = f'''
        SELECT 
            COUNT(*) as total_runs,
            MIN(run_timestamp) as first_run,
            MAX(run_timestamp) as last_run
        FROM `{test_runs_table}`
        WHERE run_timestamp >= CAST(CurrentUtcTimestamp() - {days_back}*Interval("P1D") AS Timestamp)
            AND branch = '{branch}' 
            AND build_type = '{build_type}'
            AND suite_folder = '{suite_folder}'
            AND test_name = '{test_name}'
        '''
        
        try:
            results = ydb_wrapper.execute_scan_query(query, query_name=f"diagnose_raw_{branch}")
            
            if results and results[0].get('total_runs', 0) > 0:
                total_runs = results[0].get('total_runs', 0)
                first_run = results[0].get('first_run', 0)
                last_run = results[0].get('last_run', 0)
                
                print(f"✅ Найдено {total_runs} запусков теста")
                
                # Конвертируем timestamp
                def timestamp_to_date(ts):
                    if ts is None:
                        return "N/A"
                    try:
                        if ts > 1000000000000000:  # микросекунды
                            return datetime.datetime.fromtimestamp(ts / 1000000).strftime('%Y-%m-%d %H:%M:%S')
                        elif ts > 1000000000000:  # миллисекунды
                            return datetime.datetime.fromtimestamp(ts / 1000).strftime('%Y-%m-%d %H:%M:%S')
                        else:  # секунды
                            return datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
                    except:
                        return str(ts)
                
                print(f"   Первый запуск: {timestamp_to_date(first_run)}")
                print(f"   Последний запуск: {timestamp_to_date(last_run)}")
                
                # Проверяем последние 7 дней
                today = datetime.datetime.now()
                seven_days_ago = today - datetime.timedelta(days=7)
                
                query_recent = f'''
                SELECT COUNT(*) as recent_runs
                FROM `{test_runs_table}`
                WHERE run_timestamp >= CAST(CurrentUtcTimestamp() - 7*Interval("P1D") AS Timestamp)
                    AND branch = '{branch}' 
                    AND build_type = '{build_type}'
                    AND suite_folder = '{suite_folder}'
                    AND test_name = '{test_name}'
                '''
                
                recent_results = ydb_wrapper.execute_scan_query(query_recent, query_name=f"diagnose_recent_{branch}")
                if recent_results:
                    recent_runs = recent_results[0].get('recent_runs', 0)
                    if recent_runs > 0:
                        print(f"   Запусков за последние 7 дней: {recent_runs}")
                    else:
                        print(f"   ⚠️  НЕТ запусков за последние 7 дней")
            else:
                print(f"❌ Запусков теста не найдено в test_results за последные {days_back} дней")
                
        except Exception as e:
            print(f"❌ Ошибка при выполнении запроса: {e}")


def check_test_in_testowners(test_name, suite_folder, branch, build_type='relwithdebinfo'):
    """Проверяет наличие теста в таблице testowners (критично для get_all_tests)"""
    print(f"\n{'='*80}")
    print(f"🔍 Проверка в таблице testowners (критично для get_muted_tests.py)")
    print(f"{'='*80}")
    print(f"⚠️  ВАЖНО: get_all_tests использует INNER JOIN с testowners")
    print(f"   Если теста нет в testowners, он НЕ попадет в get_all_tests")
    print(f"   и is_muted НЕ обновится!")
    
    with YDBWrapper() as ydb_wrapper:
        if not ydb_wrapper.check_credentials():
            print("❌ Не удалось проверить credentials")
            return False
        
        testowners_table = ydb_wrapper.get_table_path("testowners", database="main")
        
        query = f'''
        SELECT 
            test_name, 
            suite_folder, 
            full_name,
            run_timestamp_last
        FROM `{testowners_table}`
        WHERE suite_folder = '{suite_folder}'
            AND test_name = '{test_name}'
        LIMIT 1
        '''
        
        try:
            results = ydb_wrapper.execute_scan_query(query, query_name=f"diagnose_testowners_{branch}")
            
            if results:
                print(f"✅ Тест НАЙДЕН в testowners")
                run_timestamp_last = results[0].get('run_timestamp_last', 0)
                
                # Конвертируем timestamp
                def timestamp_to_date(ts):
                    if ts is None:
                        return "N/A"
                    try:
                        if ts > 1000000000000000:  # микросекунды
                            return datetime.datetime.fromtimestamp(ts / 1000000).strftime('%Y-%m-%d %H:%M:%S')
                        elif ts > 1000000000000:  # миллисекунды
                            return datetime.datetime.fromtimestamp(ts / 1000).strftime('%Y-%m-%d %H:%M:%S')
                        else:  # секунды
                            return datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
                    except:
                        return str(ts)
                
                print(f"   Последний запуск (из testowners): {timestamp_to_date(run_timestamp_last)}")
                return True
            else:
                print(f"❌ Тест НЕ НАЙДЕН в testowners")
                print(f"   ⚠️  Это означает, что тест НЕ попадет в get_all_tests")
                print(f"   и is_muted НЕ будет обновлен!")
                print(f"   Решение: тест должен быть в testowners (заполняется upload_testowners.py)")
                return False
                
        except Exception as e:
            print(f"❌ Ошибка при выполнении запроса: {e}")
            return False


def check_test_runs_last_90_days(test_name, suite_folder, branch, build_type='relwithdebinfo'):
    """Проверяет, запускался ли тест за последние 90 дней (критично для get_all_tests)"""
    print(f"\n{'='*80}")
    print(f"🔍 Проверка запусков за последние 90 дней (критично для get_muted_tests.py)")
    print(f"{'='*80}")
    print(f"⚠️  ВАЖНО: get_all_tests фильтрует тесты по условию:")
    print(f"   run_timestamp >= CurrentUtcDate() - 90*Interval('P1D')")
    print(f"   Если тест не запускался за последние 90 дней для ветки {branch},")
    print(f"   он НЕ попадет в get_all_tests и is_muted НЕ обновится!")
    print(f"\n📋 ТОЧНАЯ логика get_all_tests:")
    print(f"   1. INNER JOIN testowners (тест должен быть в testowners)")
    print(f"   2. Фильтр: run_timestamp >= CurrentUtcDate() - 90*Interval('P1D')")
    print(f"   3. Группировка по suite_folder, test_name")
    print(f"   4. Только для указанной ветки и build_type")
    
    with YDBWrapper() as ydb_wrapper:
        if not ydb_wrapper.check_credentials():
            print("❌ Не удалось проверить credentials")
            return False
        
        test_runs_table = ydb_wrapper.get_table_path("test_results", database="main")
        
        # Точная копия запроса из get_all_tests
        query = f'''
        SELECT 
            suite_folder,
            test_name,
            MAX(run_timestamp) as run_timestamp_last,
            COUNT(*) as total_runs
        FROM `{test_runs_table}`
        WHERE branch = '{branch}'
            AND build_type = '{build_type}'
            AND suite_folder = '{suite_folder}'
            AND test_name = '{test_name}'
            AND run_timestamp >= CurrentUtcDate() - 90*Interval("P1D")
        GROUP BY suite_folder, test_name
        '''
        
        try:
            results = ydb_wrapper.execute_scan_query(query, query_name=f"diagnose_90days_{branch}")
            
            if results and results[0].get('total_runs', 0) > 0:
                total_runs = results[0].get('total_runs', 0)
                last_run = results[0].get('run_timestamp_last', 0)
                
                print(f"✅ Тест запускался за последние 90 дней для ветки {branch}")
                print(f"   Всего запусков: {total_runs}")
                
                # Конвертируем timestamp
                def timestamp_to_date(ts):
                    if ts is None:
                        return "N/A"
                    try:
                        if ts > 1000000000000000:  # микросекунды
                            return datetime.datetime.fromtimestamp(ts / 1000000).strftime('%Y-%m-%d %H:%M:%S')
                        elif ts > 1000000000000:  # миллисекунды
                            return datetime.datetime.fromtimestamp(ts / 1000).strftime('%Y-%m-%d %H:%M:%S')
                        else:  # секунды
                            return datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
                    except:
                        return str(ts)
                
                print(f"   Последний запуск: {timestamp_to_date(last_run)}")
                
                # Проверяем, сколько дней назад был последний запуск
                if last_run > 1000000000000000:
                    last_run_seconds = last_run / 1000000
                elif last_run > 1000000000000:
                    last_run_seconds = last_run / 1000
                else:
                    last_run_seconds = last_run
                
                days_ago = (datetime.datetime.now().timestamp() - last_run_seconds) / 86400
                print(f"   Дней назад: {days_ago:.1f}")
                
                if days_ago > 90:
                    print(f"   ⚠️  ВНИМАНИЕ: Последний запуск был более 90 дней назад!")
                    print(f"   Тест НЕ попадет в get_all_tests при следующем запуске!")
                
                return True
            else:
                print(f"❌ Тест НЕ запускался за последние 90 дней для ветки {branch}")
                print(f"   ⚠️  Это означает, что тест НЕ попадет в get_all_tests")
                print(f"   и is_muted НЕ будет обновлен!")
                print(f"   Решение: дождаться запуска теста или вручную обновить is_muted")
                
                # Проверяем, когда был последний запуск вообще
                query_any = f'''
                SELECT 
                    MAX(run_timestamp) as last_run_ever
                FROM `{test_runs_table}`
                WHERE branch = '{branch}'
                    AND build_type = '{build_type}'
                    AND suite_folder = '{suite_folder}'
                    AND test_name = '{test_name}'
                '''
                
                any_results = ydb_wrapper.execute_scan_query(query_any, query_name=f"diagnose_last_ever_{branch}")
                if any_results and any_results[0].get('last_run_ever'):
                    last_ever = any_results[0].get('last_run_ever', 0)
                    def timestamp_to_date(ts):
                        if ts is None:
                            return "N/A"
                        try:
                            if ts > 1000000000000000:
                                return datetime.datetime.fromtimestamp(ts / 1000000).strftime('%Y-%m-%d %H:%M:%S')
                            elif ts > 1000000000000:
                                return datetime.datetime.fromtimestamp(ts / 1000).strftime('%Y-%m-%d %H:%M:%S')
                            else:
                                return datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
                        except:
                            return str(ts)
                    
                    print(f"   Последний запуск когда-либо: {timestamp_to_date(last_ever)}")
                
                return False
                
        except Exception as e:
            print(f"❌ Ошибка при выполнении запроса: {e}")
            return False


def simulate_get_all_tests(test_name, suite_folder, branch, build_type='relwithdebinfo'):
    """Симулирует ТОЧНЫЙ запрос get_all_tests из get_muted_tests.py"""
    print(f"Это ТОЧНАЯ копия запроса, который использует get_muted_tests.py get_all_tests")
    print(f"Если тест НЕ попадет в результат, is_muted НЕ будет обновлен!")
    
    with YDBWrapper() as ydb_wrapper:
        if not ydb_wrapper.check_credentials():
            print("❌ Не удалось проверить credentials")
            return
        
        test_runs_table = ydb_wrapper.get_table_path("test_results", database="main")
        testowners_table = ydb_wrapper.get_table_path("testowners", database="main")
        
        today = datetime.date.today().strftime('%Y-%m-%d')
        
        # ТОЧНАЯ копия запроса из get_all_tests (строки 74-94)
        tests_query = f"""
        SELECT 
            t.suite_folder as suite_folder,
            t.test_name as test_name,
            t.full_name as full_name,
            t.owners as owners,
            trc.run_timestamp_last as run_timestamp_last,
            Date('{today}') as date
        FROM `{testowners_table}` t
        INNER JOIN (
            SELECT 
                suite_folder,
                test_name,
                MAX(run_timestamp) as run_timestamp_last
            FROM `{test_runs_table}`
            WHERE branch = '{branch}'
            AND build_type = '{build_type}'
            and  run_timestamp >= CurrentUtcDate() - 90*Interval("P1D")
            GROUP BY suite_folder, test_name
        ) trc ON t.suite_folder = trc.suite_folder AND t.test_name = trc.test_name
        WHERE t.suite_folder = '{suite_folder}'
            AND t.test_name = '{test_name}'
        """
        
        print(f"\n📊 SQL запрос (из get_all_tests):\n{tests_query}\n")
        
        try:
            results = ydb_wrapper.execute_scan_query(tests_query, query_name=f"simulate_get_all_tests_{branch}")
            
            if results:
                print(f"✅ Тест ПОПАДЕТ в get_all_tests!")
                print(f"   Найдено {len(results)} записей")
                print(f"   Это означает, что is_muted БУДЕТ обновлен при запуске get_muted_tests.py")
                
                for row in results:
                    print(f"\n   Результат:")
                    print(f"   - suite_folder: {row.get('suite_folder')}")
                    print(f"   - test_name: {row.get('test_name')}")
                    print(f"   - full_name: {row.get('full_name')}")
                    print(f"   - owners: {row.get('owners', 'N/A')}")
                    run_ts = row.get('run_timestamp_last', 0)
                    if run_ts:
                        def timestamp_to_date(ts):
                            try:
                                if ts > 1000000000000000:
                                    return datetime.datetime.fromtimestamp(ts / 1000000).strftime('%Y-%m-%d %H:%M:%S')
                                elif ts > 1000000000000:
                                    return datetime.datetime.fromtimestamp(ts / 1000).strftime('%Y-%m-%d %H:%M:%S')
                                else:
                                    return datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
                            except:
                                return str(ts)
                        print(f"   - run_timestamp_last: {timestamp_to_date(run_ts)}")
            else:
                print(f"❌ Тест НЕ ПОПАДЕТ в get_all_tests!")
                print(f"   Это означает, что is_muted НЕ будет обновлен при запуске get_muted_tests.py")
                print(f"\n   Возможные причины:")
                print(f"   1. Тест отсутствует в testowners (INNER JOIN не сработает)")
                print(f"   2. Тест не запускался за последние 90 дней для ветки {branch}")
                print(f"   3. Несоответствие suite_folder или test_name")
                
        except Exception as e:
            print(f"❌ Ошибка при выполнении запроса: {e}")
            import traceback
            traceback.print_exc()


def check_test_in_all_tests_with_owner(test_name, suite_folder, branch, build_type='relwithdebinfo', days_back=30):
    """Проверяет наличие теста в таблице all_tests_with_owner_and_mute"""
    print(f"\n{'='*80}")
    print(f"🔍 Проверка в таблице all_tests_with_owner_and_mute")
    print(f"{'='*80}")
    
    with YDBWrapper() as ydb_wrapper:
        if not ydb_wrapper.check_credentials():
            print("❌ Не удалось проверить credentials")
            return
        
        all_tests_table = ydb_wrapper.get_table_path("all_tests_with_owner_and_mute", database="main")
        
        query = f'''
        SELECT 
            test_name, 
            suite_folder, 
            full_name, 
            branch, 
            date,
            owners,
            is_muted
        FROM `{all_tests_table}`
        WHERE date >= CurrentUtcDate() - {days_back}*Interval("P1D")
            AND branch = '{branch}' 
            AND suite_folder = '{suite_folder}'
            AND test_name = '{test_name}'
        ORDER BY date DESC
        LIMIT 20
        '''
        
        try:
            results = ydb_wrapper.execute_scan_query(query, query_name=f"diagnose_all_tests_{branch}")
            
            if results:
                print(f"✅ Найдено {len(results)} записей в all_tests_with_owner_and_mute")
                print(f"\n{'Дата':<12} {'Owner':<40} {'Muted':<8}")
                print("-" * 80)
                for row in results[:10]:
                    date = row.get('date', 'N/A')
                    if isinstance(date, int):
                        base_date = datetime.date(1970, 1, 1)
                        date = (base_date + datetime.timedelta(days=date)).strftime('%Y-%m-%d')
                    owner = row.get('owners', 'N/A')
                    if isinstance(owner, bytes):
                        owner = owner.decode('utf-8')
                    owner = (owner[:37] + '...') if len(str(owner)) > 40 else owner
                    print(f"{date:<12} {owner:<40} {row.get('is_muted', 0):<8}")
            else:
                print(f"❌ Записей не найдено в all_tests_with_owner_and_mute за последние {days_back} дней")
                print(f"   Это может означать, что тест не был собран в эту таблицу")
                
        except Exception as e:
            print(f"❌ Ошибка при выполнении запроса: {e}")


def check_mute_check_filter(test_name, suite_folder, muted_ya_path=None, ydb_is_muted=None):
    """Проверяет, проходит ли тест через фильтр mute_check (YaMuteCheck)"""
    print(f"\n{'='*80}")
    print(f"🔍 Проверка фильтра mute_check (YaMuteCheck)")
    print(f"{'='*80}")
    
    if muted_ya_path is None:
        # Пытаемся найти muted_ya.txt в стандартном месте
        script_dir = os.path.dirname(os.path.abspath(__file__))
        repo_path = os.path.join(script_dir, '../../../')
        muted_ya_path = os.path.join(repo_path, '.github/config/muted_ya.txt')
    
    if not os.path.exists(muted_ya_path):
        print(f"⚠️  Файл muted_ya.txt не найден: {muted_ya_path}")
        print(f"   Пропускаем проверку фильтра")
        return
    
    try:
        mute_check = YaMuteCheck()
        mute_check.load(muted_ya_path)
        print(f"✅ Загружено {len(mute_check.regexps)} паттернов из {muted_ya_path}")
        
        # Проверяем, проходит ли тест через фильтр
        passes = mute_check(suite_folder, test_name)
        
        # Проверяем несоответствие с YDB
        if ydb_is_muted is not None:
            print(f"\n⚠️  ПРОВЕРКА СИНХРОНИЗАЦИИ:")
            print(f"   YDB is_muted: {ydb_is_muted}")
            print(f"   muted_ya.txt: {'есть' if passes else 'нет'}")
            
            if ydb_is_muted == 1 and not passes:
                print(f"\n❌ НЕСООТВЕТСТВИЕ ОБНАРУЖЕНО!")
                print(f"   Тест помечен как замьюченный в YDB (is_muted=1), но его НЕТ в muted_ya.txt")
                print(f"   Возможные причины:")
                print(f"   1. Тест был удален из muted_ya.txt, но не запускался недавно")
                print(f"   2. Скрипт get_muted_tests.py не обновил is_muted (тест не попал в get_all_tests)")
                print(f"   3. Рассинхронизация между muted_ya.txt и YDB")
                print(f"   Решение: запустить get_muted_tests.py upload_muted_tests для этой ветки")
            elif ydb_is_muted == 0 and passes:
                print(f"\n⚠️  НЕСООТВЕТСТВИЕ ОБНАРУЖЕНО!")
                print(f"   Тест есть в muted_ya.txt, но is_muted=0 в YDB")
                print(f"   Возможные причины:")
                print(f"   1. muted_ya.txt был обновлен, но get_muted_tests.py еще не запускался")
                print(f"   2. Тест не попал в get_all_tests (не запускался недавно)")
            elif ydb_is_muted == 1 and passes:
                print(f"\n✅ СИНХРОНИЗАЦИЯ: Тест замьючен и в YDB, и в muted_ya.txt")
            elif ydb_is_muted == 0 and not passes:
                print(f"\n✅ СИНХРОНИЗАЦИЯ: Тест не замьючен ни в YDB, ни в muted_ya.txt")
        
        if passes:
            print(f"\n✅ Тест ПРОХОДИТ через фильтр mute_check")
            print(f"   Это означает, что тест должен обрабатываться скриптом")
        else:
            print(f"\n❌ Тест НЕ ПРОХОДИТ через фильтр mute_check")
            print(f"   Это означает, что тест будет ПРОПУЩЕН в create_file_set")
            print(f"   Тест не будет включен в результаты, даже если есть в YDB")
            
            # Пытаемся найти похожие паттерны
            print(f"\n   Похожие паттерны в muted_ya.txt:")
            found_similar = False
            with open(muted_ya_path, 'r') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue
                    try:
                        pat_suite, pat_test = line.split(" ", maxsplit=1)
                        if suite_folder in pat_suite or pat_suite in suite_folder:
                            if test_name in pat_test or pat_test in test_name:
                                print(f"      Строка {line_num}: {line}")
                                found_similar = True
                    except:
                        pass
            if not found_similar:
                print(f"      (похожих паттернов не найдено)")
    except Exception as e:
        print(f"❌ Ошибка при проверке фильтра: {e}")
        import traceback
        traceback.print_exc()


def simulate_execute_query(test_name, suite_folder, branch, build_type='relwithdebinfo'):
    """Симулирует точный запрос из execute_query в create_new_muted_ya.py"""
    print(f"\n{'='*80}")
    print(f"🔍 Симуляция запроса execute_query (как в create_new_muted_ya.py)")
    print(f"{'='*80}")
    print(f"Это ТОЧНАЯ копия запроса, который использует create_new_muted_ya.py")
    
    with YDBWrapper() as ydb_wrapper:
        if not ydb_wrapper.check_credentials():
            print("❌ Не удалось проверить credentials")
            return
        
        tests_monitor_table = ydb_wrapper.get_table_path("tests_monitor", database="main")
        
        # ТОЧНАЯ копия запроса из execute_query
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
        AND suite_folder = '{suite_folder}'
        AND test_name = '{test_name}'
    '''
        
        print(f"\n📊 SQL запрос (из execute_query):\n{query_string}\n")
        
        try:
            results = ydb_wrapper.execute_scan_query(query_string, query_name=f"simulate_execute_query_{branch}")
            
            if results:
                print(f"✅ Найдено {len(results)} записей в результате execute_query")
                print(f"   Тест БУДЕТ включен в all_data для обработки")
                
                # Показываем детали
                print(f"\n{'Дата':<12} {'Pass':<6} {'Fail':<6} {'Mute':<6} {'State':<20} {'Muted':<8}")
                print("-" * 80)
                for row in results[:5]:
                    date = row.get('date_window', 'N/A')
                    if isinstance(date, int):
                        base_date = datetime.date(1970, 1, 1)
                        date = (base_date + datetime.timedelta(days=date)).strftime('%Y-%m-%d')
                    print(f"{date:<12} {row.get('pass_count', 0):<6} {row.get('fail_count', 0):<6} "
                          f"{row.get('mute_count', 0):<6} {row.get('state', 'N/A'):<20} {row.get('is_muted', 0):<8}")
            else:
                print(f"❌ Записей НЕ найдено в результате execute_query")
                print(f"   Это означает, что тест НЕ будет включен в all_data")
                print(f"   Причина: нет данных в tests_monitor за последние 7 дней")
                
        except Exception as e:
            print(f"❌ Ошибка при выполнении запроса: {e}")
            import traceback
            traceback.print_exc()


def check_branch_data_summary(branch, build_type='relwithdebinfo'):
    """Проверяет общую статистику по ветке"""
    print(f"\n{'='*80}")
    print(f"📊 Общая статистика по ветке {branch}")
    print(f"{'='*80}")
    
    with YDBWrapper() as ydb_wrapper:
        if not ydb_wrapper.check_credentials():
            print("❌ Не удалось проверить credentials")
            return
        
        tests_monitor_table = ydb_wrapper.get_table_path("tests_monitor", database="main")
        
        # Проверяем последние 7 дней
        query = f'''
        SELECT 
            COUNT(*) as total_tests,
            COUNT(DISTINCT full_name) as unique_tests,
            MIN(date_window) as earliest_date,
            MAX(date_window) as latest_date
        FROM `{tests_monitor_table}`
        WHERE date_window >= CurrentUtcDate() - 7*Interval("P1D")
            AND branch = '{branch}' 
            AND build_type = '{build_type}'
        '''
        
        try:
            results = ydb_wrapper.execute_scan_query(query, query_name=f"branch_summary_{branch}")
            
            if results:
                row = results[0]
                total_tests = row.get('total_tests', 0)
                unique_tests = row.get('unique_tests', 0)
                earliest = row.get('earliest_date')
                latest = row.get('latest_date')
                
                def date_to_str(d):
                    if d is None:
                        return "N/A"
                    if isinstance(d, int):
                        base_date = datetime.date(1970, 1, 1)
                        return (base_date + datetime.timedelta(days=d)).strftime('%Y-%m-%d')
                    return str(d)
                
                print(f"Всего записей за последние 7 дней: {total_tests}")
                print(f"Уникальных тестов: {unique_tests}")
                print(f"Самая ранняя дата: {date_to_str(earliest)}")
                print(f"Самая поздняя дата: {date_to_str(latest)}")
                
                if total_tests == 0:
                    print(f"\n⚠️  ВНИМАНИЕ: Нет данных для ветки {branch} за последние 7 дней!")
                    print(f"   Это означает, что tests_monitor.py не собирал данные для этой ветки")
            else:
                print(f"❌ Не удалось получить статистику")
                
        except Exception as e:
            print(f"❌ Ошибка при выполнении запроса: {e}")


def generate_fix_commands(branch, build_type, issues):
    """Генерирует конкретные команды для исправления проблем"""
    print(f"\n{'='*80}")
    print(f"🔧 КОМАНДЫ ДЛЯ ИСПРАВЛЕНИЯ")
    print(f"{'='*80}")
    
    if not issues:
        print(f"✅ Проблем не обнаружено, команды не требуются")
        return
    
    print(f"\n📋 Выполните следующие команды для исправления:")
    print(f"\n1. Обновить is_muted в YDB на основе текущего muted_ya.txt:")
    print(f"   python3 .github/scripts/tests/get_muted_tests.py upload_muted_tests \\")
    print(f"     --branch {branch} \\")
    print(f"     --build_type {build_type}")
    
    if 'not_in_testowners' in issues:
        print(f"\n2. Обновить testowners (если тест отсутствует в testowners):")
        print(f"   python3 .github/scripts/analytics/upload_testowners.py")
    
    if 'not_runs_90_days' in issues:
        print(f"\n3. Дождаться запуска теста (если не запускался >90 дней):")
        print(f"   Тест должен запуститься в CI для ветки {branch}")
        print(f"   После запуска повторить команду из пункта 1")
    
    if 'needs_sync' in issues:
        print(f"\n4. Проверить синхронизацию после исправления:")
        print(f"   python3 .github/scripts/tests/check_mute_sync.py \\")
        print(f"     --branch {branch} \\")
        print(f"     --build_type {build_type} \\")
        print(f"     --days 1")
    
    print(f"\n{'='*80}\n")


def main():
    parser = argparse.ArgumentParser(
        description="Диагностика наличия теста в YDB для конкретной ветки. "
                    "Проверяет все этапы обработки данных в create_new_muted_ya.py. "
                    "Показывает конкретные причины проблем и команды для исправления."
    )
    parser.add_argument('--test_name', required=True, 
                      help='Имя теста (например: TestInsert.test_multi[read_data_during_bulk_upsert])')
    parser.add_argument('--suite_folder', required=True, 
                      help='Папка сьюта (например: ydb/tests/olap/scenario)')
    parser.add_argument('--branch', required=True, 
                      help='Ветка (например: stable-25-3)')
    parser.add_argument('--build_type', default='relwithdebinfo', 
                      help='Тип сборки (default: relwithdebinfo)')
    parser.add_argument('--days_back', type=int, default=30, 
                      help='Сколько дней назад проверять (default: 30)')
    parser.add_argument('--muted_ya_file', 
                      help='Путь к файлу muted_ya.txt (по умолчанию: .github/config/muted_ya.txt)')
    
    args = parser.parse_args()
    
    print(f"\n{'#'*80}")
    print(f"🔬 ДИАГНОСТИКА ТЕСТА В YDB")
    print(f"{'#'*80}")
    print(f"Test: {args.suite_folder} {args.test_name}")
    print(f"Branch: {args.branch}")
    print(f"Build type: {args.build_type}")
    print(f"Period: последние {args.days_back} дней")
    
    # Сначала проверяем общую статистику по ветке
    check_branch_data_summary(args.branch, args.build_type)
    
    # Затем проверяем конкретный тест по всем таблицам в порядке обработки
    print(f"\n{'#'*80}")
    print(f"📋 ПОРЯДОК ПРОВЕРКИ (как в create_new_muted_ya.py):")
    print(f"{'#'*80}")
    print(f"1. test_results - сырые данные о запусках")
    print(f"2. flaky_tests_window - агрегированная история (заполняется из test_results)")
    print(f"3. all_tests_with_owner_and_mute - тесты с владельцами (используется tests_monitor.py)")
    print(f"4. tests_monitor - финальная таблица (заполняется из flaky_tests_window + all_tests)")
    print(f"5. execute_query - запрос из create_new_muted_ya.py (фильтр: последние 7 дней)")
    print(f"6. mute_check фильтр - проверка по muted_ya.txt")
    print(f"{'#'*80}\n")
    
    # 0. КРИТИЧЕСКИЕ ПРОВЕРКИ для get_muted_tests.py (должны быть первыми!)
    print(f"\n{'#'*80}")
    print(f"🔴 КРИТИЧЕСКИЕ ПРОВЕРКИ для get_muted_tests.py")
    print(f"{'#'*80}")
    print(f"Эти проверки объясняют, почему is_muted может не обновиться:")
    print(f"1. Тест должен быть в testowners (INNER JOIN)")
    print(f"2. Тест должен запускаться за последние 90 дней для ветки")
    print(f"{'#'*80}\n")
    
    in_testowners = check_test_in_testowners(args.test_name, args.suite_folder, args.branch, args.build_type)
    runs_last_90_days = check_test_runs_last_90_days(args.test_name, args.suite_folder, args.branch, args.build_type)
    
    # Симуляция точного запроса get_all_tests
    print(f"\n{'='*80}")
    print(f"🔍 СИМУЛЯЦИЯ точного запроса get_all_tests")
    print(f"{'='*80}")
    simulate_get_all_tests(args.test_name, args.suite_folder, args.branch, args.build_type)
    
    if not in_testowners or not runs_last_90_days:
        print(f"\n⚠️  ВНИМАНИЕ: Тест НЕ попадет в get_all_tests!")
        print(f"   Причина:")
        if not in_testowners:
            print(f"   - Тест отсутствует в testowners")
        if not runs_last_90_days:
            print(f"   - Тест не запускался за последние 90 дней для ветки {args.branch}")
        print(f"   Следствие: is_muted НЕ будет обновлен при запуске get_muted_tests.py")
        print(f"   Решение: дождаться запуска теста или вручную обновить is_muted")
    
    # 1. Сырые данные
    check_test_in_test_results(args.test_name, args.suite_folder, args.branch, args.build_type, args.days_back)
    
    # 2. Агрегированная история
    check_test_in_flaky_tests_window(args.test_name, args.suite_folder, args.branch, args.build_type, args.days_back)
    
    # 3. Тесты с владельцами
    check_test_in_all_tests_with_owner(args.test_name, args.suite_folder, args.branch, args.build_type, args.days_back)
    
    # 4. Финальная таблица мониторинга (получаем is_muted для проверки синхронизации)
    ydb_is_muted = check_test_in_tests_monitor(args.test_name, args.suite_folder, args.branch, args.build_type, args.days_back)
    
    # 5. Симуляция точного запроса из create_new_muted_ya.py
    simulate_execute_query(args.test_name, args.suite_folder, args.branch, args.build_type)
    
    # 6. Проверка фильтра mute_check с проверкой синхронизации
    # Используем переданный путь или пытаемся найти muted_ya.txt
    if args.muted_ya_file:
        muted_ya_path = args.muted_ya_file
    else:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        repo_path = os.path.join(script_dir, '../../../')
        muted_ya_path = os.path.join(repo_path, '.github/config/muted_ya.txt')
    check_mute_check_filter(args.test_name, args.suite_folder, muted_ya_path, ydb_is_muted)
    
    # Итоговая сводка с конкретными рекомендациями
    print(f"\n{'#'*80}")
    print(f"📊 ИТОГОВАЯ СВОДКА И РЕКОМЕНДАЦИИ")
    print(f"{'#'*80}")
    
    # Анализируем результаты проверок
    print(f"\n🔍 РЕЗУЛЬТАТЫ ДИАГНОСТИКИ:")
    
    # Проверяем, что мы знаем о тесте
    issues_found = []
    solutions = []
    
    if ydb_is_muted is None:
        print(f"  ⚠️  Тест не найден в tests_monitor за последние {args.days_back} дней")
        issues_found.append("Нет данных в tests_monitor")
        solutions.append("Проверить, запускался ли тест для ветки " + args.branch)
    elif ydb_is_muted == 1:
        print(f"  ✅ Тест найден в tests_monitor с is_muted=1")
    else:
        print(f"  ⚠️  Тест найден в tests_monitor с is_muted=0")
        issues_found.append("Тест не замьючен в YDB")
        solutions.append("Запустить get_muted_tests.py upload_muted_tests для ветки " + args.branch)
    
    # Проверяем mute_check
    script_dir = os.path.dirname(os.path.abspath(__file__))
    repo_path = os.path.join(script_dir, '../../../')
    if args.muted_ya_file:
        muted_ya_path = args.muted_ya_file
    else:
        muted_ya_path = os.path.join(repo_path, '.github/config/muted_ya.txt')
    
    passes_filter = False
    if os.path.exists(muted_ya_path):
        mute_check = YaMuteCheck()
        mute_check.load(muted_ya_path)
        passes_filter = mute_check(args.suite_folder, args.test_name)
        if passes_filter:
            print(f"  ✅ Тест проходит через mute_check фильтр (есть в muted_ya.txt)")
        else:
            print(f"  ❌ Тест НЕ проходит через mute_check фильтр (нет в muted_ya.txt)")
            if ydb_is_muted == 1:
                issues_found.append("Тест замьючен в YDB, но отсутствует в muted_ya.txt")
                solutions.append("Добавить тест в muted_ya.txt или запустить get_muted_tests.py для обновления is_muted")
    
    # Проверяем критические условия
    issues_dict = {}
    if not in_testowners:
        issues_found.append("Тест отсутствует в testowners")
        solutions.append("Обновить testowners: python3 .github/scripts/analytics/upload_testowners.py")
        issues_dict['not_in_testowners'] = True
    
    if not runs_last_90_days:
        issues_found.append("Тест не запускался за последние 90 дней для ветки")
        solutions.append("Дождаться запуска теста в CI для ветки " + args.branch)
        issues_dict['not_runs_90_days'] = True
    
    if issues_found:
        issues_dict['needs_sync'] = True
    
    print(f"\n🔴 ОБНАРУЖЕННЫЕ ПРОБЛЕМЫ:")
    if issues_found:
        for i, issue in enumerate(issues_found, 1):
            print(f"  {i}. {issue}")
    else:
        print(f"  ✅ Проблем не обнаружено")
    
    print(f"\n💡 РЕКОМЕНДУЕМЫЕ ДЕЙСТВИЯ:")
    if solutions:
        for i, solution in enumerate(solutions, 1):
            print(f"  {i}. {solution}")
    else:
        print(f"  ✅ Действия не требуются")
    
    # Генерируем конкретные команды для исправления
    if issues_found:
        generate_fix_commands(args.branch, args.build_type, issues_dict)
    
    print(f"\n📋 ОБЩИЕ ПРОВЕРКИ:")
    print(f"  ✓ Есть ли данные в test_results за последние 7 дней")
    print(f"  ✓ Есть ли данные в tests_monitor за последние 7 дней (фильтр execute_query)")
    print(f"  ✓ Проходит ли тест через mute_check фильтр (если нет - будет пропущен)")
    print(f"  ✓ Запускался ли тест для ветки {args.branch} вообще")
    print(f"\n🔴 ПРИЧИНЫ РАССИНХРОНИЗАЦИИ is_muted между YDB и muted_ya.txt:")
    print(f"  1. Тест отсутствует в testowners → не попадет в get_all_tests")
    print(f"  2. Тест не запускался за последние 90 дней для ветки → не попадет в get_all_tests")
    print(f"  3. get_muted_tests.py не запускался после изменения muted_ya.txt")
    print(f"  4. Тест был удален из muted_ya.txt, но не запускался → is_muted остался старым")
    print(f"\n💡 РЕШЕНИЕ рассинхронизации:")
    print(f"  - Запустить get_muted_tests.py upload_muted_tests для ветки {args.branch}")
    print(f"  - Дождаться запуска теста (если не запускался >90 дней)")
    print(f"  - Убедиться, что тест есть в testowners (заполняется upload_testowners.py)")
    print(f"{'#'*80}\n")


if __name__ == "__main__":
    main()

