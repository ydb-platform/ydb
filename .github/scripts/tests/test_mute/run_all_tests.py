#!/usr/bin/env python3
"""
Запускает все тесты для create_new_muted_ya.py
"""

import unittest
import sys
import os

# Добавляем текущую директорию в path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Импортируем все тесты
from test_create_new_muted_ya_consolidated import TestCreateNewMutedYaConsolidated
from test_module_create_new_muted_ya.test_update_muted_ya import TestUpdateMutedYaIntegration
from test_module_create_new_muted_ya.test_create_issues import TestCreateIssues

def run_all_tests():
    """Запускает все тесты"""
    
    # Создаем test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Добавляем основные тесты
    suite.addTests(loader.loadTestsFromTestCase(TestCreateNewMutedYaConsolidated))
    
    # Добавляем тесты интеграции
    suite.addTests(loader.loadTestsFromTestCase(TestUpdateMutedYaIntegration))
    
    # Добавляем тесты создания issues
    suite.addTests(loader.loadTestsFromTestCase(TestCreateIssues))
    
    # Запускаем тесты
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Выводим результат
    if result.wasSuccessful():
        print("\n✅ Все тесты прошли успешно!")
        return 0
    else:
        print(f"\n❌ Провалено тестов: {len(result.failures)}")
        print(f"❌ Ошибок: {len(result.errors)}")
        return 1

if __name__ == "__main__":
    sys.exit(run_all_tests()) 