#!/usr/bin/env python3

import unittest
import tempfile
import os
import sys
from unittest.mock import patch

# Настройка моков
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from mock_setup import setup_mocks
setup_mocks()


class TestYaMuteCheck(unittest.TestCase):
    """Тесты для класса YaMuteCheck из transform_ya_junit.py"""

    def setUp(self):
        """Настройка тестов"""
        from transform_ya_junit import YaMuteCheck
        self.mute_check = YaMuteCheck()

    def test_basic_functionality(self):
        """Тест базовой функциональности YaMuteCheck"""
        # Тестируем добавление различных паттернов
        self.mute_check.populate("ydb/tests/functional/*", "test_*.py")
        self.mute_check.populate("ydb/core/tx/schemeshard", "ut_*.cpp*")
        self.mute_check.populate("exact/path", "exact_test.py")
        
        # Тестируем точные совпадения
        self.assertTrue(self.mute_check("exact/path", "exact_test.py"))
        
        # Тестируем wildcard совпадения
        self.assertTrue(self.mute_check("ydb/tests/functional/tpc", "test_example.py"))
        self.assertTrue(self.mute_check("ydb/tests/functional/canonical", "test_sql.py"))
        self.assertTrue(self.mute_check("ydb/core/tx/schemeshard", "ut_scheme_shard.cpp::TestName"))
        
        # Тестируем несовпадения
        self.assertFalse(self.mute_check("ydb/tests/functional/tpc", "example.py"))  # не test_*
        self.assertFalse(self.mute_check("ydb/other/path", "test_example.py"))  # неправильный путь
        self.assertFalse(self.mute_check("exact/path", "other_test.py"))  # не точное совпадение

    def test_load_from_file(self):
        """Тест загрузки YaMuteCheck из файла"""
        # Создаем тестовый файл с правилами
        test_content = """ydb/tests/functional/tpc/large test_tpcds.py.TestTpcdsS1.test_tpcds[14]
ydb/core/tx/schemeshard ut_scheme_shard.cpp::TSchemeShardTest.CreateTable
ydb/tests/functional/* test_*.py
invalid_line_without_space
# comment line
ydb/exact/path exact_test.name"""
        
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write(test_content)
            test_file_path = f.name
        
        try:
            with patch('transform_ya_junit.log_print') as mock_log:
                self.mute_check.load(test_file_path)
            
            # Проверяем, что корректные строки загружены
            self.assertTrue(self.mute_check("ydb/tests/functional/tpc/large", "test_tpcds.py.TestTpcdsS1.test_tpcds[14]"))
            self.assertTrue(self.mute_check("ydb/core/tx/schemeshard", "ut_scheme_shard.cpp::TSchemeShardTest.CreateTable"))
            self.assertTrue(self.mute_check("ydb/tests/functional/anything", "test_something.py"))
            self.assertTrue(self.mute_check("ydb/exact/path", "exact_test.name"))
            
            # Проверяем, что некорректные строки пропущены
            self.assertFalse(self.mute_check("invalid", "line"))
            
            # Проверяем, что было предупреждение о некорректной строке
            mock_log.assert_called()
            
        finally:
            os.unlink(test_file_path)

    def test_empty_initialization(self):
        """Тест что YaMuteCheck работает с пустой инициализацией"""
        from transform_ya_junit import YaMuteCheck
        empty_check = YaMuteCheck()
        
        # Проверяем что пустой checker ничего не матчит
        self.assertFalse(empty_check("any/path", "any_test"))

    def test_invalid_regex_handling(self):
        """Тест обработки некорректных регулярных выражений"""
        with patch('transform_ya_junit.log_print') as mock_log:
            # Мокируем pattern_to_re чтобы вернуть невалидный regex
            with patch('transform_ya_junit.pattern_to_re') as mock_pattern:
                mock_pattern.return_value = "(?P<invalid"  # Невалидный regex - незакрытая группа
                
                # Пытаемся добавить паттерн который приведет к ошибке regex
                self.mute_check.populate("suite", "test")
                
                # Проверяем что было залогировано предупреждение
                mock_log.assert_called()

    def test_multiple_patterns_same_test(self):
        """Тест что один тест может матчиться несколькими паттернами"""
        self.mute_check.populate("ydb/tests/*", "*")
        self.mute_check.populate("ydb/tests/functional/*", "test_*.py")
        
        # Оба паттерна должны матчить этот тест
        self.assertTrue(self.mute_check("ydb/tests/functional/example", "test_case.py"))


if __name__ == '__main__':
    unittest.main(verbosity=2) 