#!/usr/bin/env python3
"""
Главный файл для запуска всех тестов в test_mute
"""

import unittest
import sys
import os

# Добавляем путь к общим утилитам
sys.path.insert(0, os.path.dirname(__file__))

# Настройка моков перед импортом любых модулей
from mock_setup import setup_mocks
setup_mocks()

def run_all_tests(mute_rules_only=False):
    """Запускает все тесты в правильном порядке"""
    
    if mute_rules_only:
        # Только тесты для mute rules
        test_modules = [
            'test_mute_rules',
            'test_mute_rules_detailed',
            'test_mute_rules_summary',
        ]
    else:
        # Определяем порядок запуска тестов
        test_modules = [
            # Базовые тесты для transform_ya_junit
            'test_module_transform_ya_junit.test_ya_mute_check',
            
            # Тесты для create_new_muted_ya
            'test_module_create_new_muted_ya.test_update_muted_ya',
            'test_module_create_new_muted_ya.test_create_issues',
            'test_module_create_new_muted_ya.test_historical_data',
            'test_module_create_new_muted_ya.test_historical_query',
            'test_module_create_new_muted_ya.test_sql_modifications',
            'test_module_create_new_muted_ya.test_boundary_conditions',
            
            # Тесты для update_mute_issues
            'test_module_update_mute_issues.test_issues_management',
            
            # Тесты для форматирования
            'test_module_formatting.test_output_formatting',
            
            # Тесты для mute rules
            'test_mute_rules',
            'test_mute_rules_detailed',
            'test_mute_rules_summary',
        ]
    
    # Создаем test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Добавляем тесты в suite
    for module_name in test_modules:
        try:
            module = __import__(module_name, fromlist=[''])
            tests = loader.loadTestsFromModule(module)
            suite.addTests(tests)
            print(f"✓ Загружены тесты из {module_name}")
        except Exception as e:
            print(f"✗ Ошибка загрузки тестов из {module_name}: {e}")
    
    # Запускаем тесты
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Выводим сводку
    print(f"\n{'='*60}")
    print(f"РЕЗУЛЬТАТЫ ТЕСТИРОВАНИЯ")
    print(f"{'='*60}")
    print(f"Запущено тестов: {result.testsRun}")
    print(f"Успешно: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"Неудачи: {len(result.failures)}")
    print(f"Ошибки: {len(result.errors)}")
    
    if result.failures:
        print(f"\nНЕУДАЧИ:")
        for test, traceback in result.failures:
            print(f"  - {test}: {traceback.split(chr(10))[-2]}")
    
    if result.errors:
        print(f"\nОШИБКИ:")
        for test, traceback in result.errors:
            print(f"  - {test}: {traceback.split(chr(10))[-2]}")
    
    # Возвращаем код выхода
    return 0 if result.wasSuccessful() else 1

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Запуск тестов для test_mute')
    parser.add_argument('--mute-rules-only', action='store_true', 
                        help='Запустить только тесты для mute rules')
    
    args = parser.parse_args()
    
    sys.exit(run_all_tests(mute_rules_only=args.mute_rules_only)) 