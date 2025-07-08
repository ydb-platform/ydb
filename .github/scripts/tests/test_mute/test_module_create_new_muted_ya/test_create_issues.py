#!/usr/bin/env python3

import unittest
import tempfile
import os
import sys
from unittest.mock import patch, MagicMock, mock_open
import datetime

# Настройка моков
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from mock_setup import setup_mocks
setup_mocks()


class TestCreateIssues(unittest.TestCase):
    """Тесты для функциональности create_issues в create_new_muted_ya.py"""

    def setUp(self):
        """Настройка тестов"""
        self.sample_tests_data = [
            {
                'full_name': 'ydb/tests/functional/tpc/large/test1',
                'suite_folder': 'ydb/tests/functional/tpc/large',
                'test_name': 'test1',
                'owner': 'team/topics',
                'success_rate': 45.5,
                'days_in_state': 5,
                'state': 'Flaky',
                'date_window': 19723,  # Дни с 1970-01-01
                'summary': 'Test summary',
                'fail_count': 15,
                'pass_count': 10,
                'branch': 'main'
            },
            {
                'full_name': 'ydb/core/tx/schemeshard/test2',
                'suite_folder': 'ydb/core/tx/schemeshard',
                'test_name': 'test2',
                'owner': 'team/topics',
                'success_rate': 30.0,
                'days_in_state': 8,
                'state': 'Muted Stable',
                'date_window': 19723,
                'summary': 'Another test summary',
                'fail_count': 20,
                'pass_count': 5,
                'branch': 'main'
            }
        ]

    @patch('create_new_muted_ya.get_muted_tests_from_issues')
    @patch('create_new_muted_ya.close_unmuted_issues')
    @patch('create_new_muted_ya.create_and_add_issue_to_project')
    @patch('create_new_muted_ya.generate_github_issue_title_and_body')
    @patch('create_new_muted_ya.read_tests_from_file')
    def test_create_mute_issues_basic(self, mock_read_tests, mock_generate, mock_create_issue, mock_close_issues, mock_get_muted):
        """Тест основной функциональности create_mute_issues"""
        from create_new_muted_ya import create_mute_issues
        
        # Настройка моков
        mock_read_tests.return_value = [
            {'testsuite': 'ydb/tests/functional/tpc/large', 'testcase': 'test1', 'full_name': 'ydb/tests/functional/tpc/large/test1'},
            {'testsuite': 'ydb/core/tx/schemeshard', 'testcase': 'test2', 'full_name': 'ydb/core/tx/schemeshard/test2'}
        ]
        
        mock_get_muted.return_value = {}  # Нет уже существующих issue
        mock_close_issues.return_value = ([], [])  # Нет закрытых issues
        mock_generate.return_value = ('Test Issue Title', 'Test Issue Body')
        mock_create_issue.return_value = {'issue_url': 'https://github.com/test/issue/1'}
        
        # Тест
        create_mute_issues(self.sample_tests_data, '/tmp/test_file.txt', close_issues=True)
        
        # Проверки
        mock_read_tests.assert_called_once_with('/tmp/test_file.txt')
        mock_get_muted.assert_called_once()
        mock_close_issues.assert_called_once()
        mock_generate.assert_called()
        mock_create_issue.assert_called()

    @patch('create_new_muted_ya.get_muted_tests_from_issues')
    @patch('create_new_muted_ya.close_unmuted_issues')
    @patch('create_new_muted_ya.create_and_add_issue_to_project')
    @patch('create_new_muted_ya.generate_github_issue_title_and_body')
    @patch('create_new_muted_ya.read_tests_from_file')
    def test_create_mute_issues_with_existing_issues(self, mock_read_tests, mock_generate, mock_create_issue, mock_close_issues, mock_get_muted):
        """Тест что тесты с существующими issue не создают новые"""
        from create_new_muted_ya import create_mute_issues
        
        # Настройка моков
        mock_read_tests.return_value = [
            {'testsuite': 'ydb/tests/functional/tpc/large', 'testcase': 'test1', 'full_name': 'ydb/tests/functional/tpc/large/test1'}
        ]
        
        # Тест уже имеет issue
        mock_get_muted.return_value = {
            'ydb/tests/functional/tpc/large/test1': [{'url': 'https://github.com/test/existing/1'}]
        }
        
        mock_close_issues.return_value = ([], [])
        
        with patch('create_new_muted_ya.logging.info') as mock_log:
            create_mute_issues(self.sample_tests_data, '/tmp/test_file.txt', close_issues=True)
        
        # Проверяем что было залогировано сообщение о существующем issue
        mock_log.assert_called()
        # Проверяем что новые issue не создавались
        mock_create_issue.assert_not_called()

    @patch('create_new_muted_ya.get_muted_tests_from_issues')
    @patch('create_new_muted_ya.close_unmuted_issues')
    @patch('create_new_muted_ya.create_and_add_issue_to_project')
    @patch('create_new_muted_ya.generate_github_issue_title_and_body')
    @patch('create_new_muted_ya.read_tests_from_file')
    def test_create_mute_issues_chunking(self, mock_read_tests, mock_generate, mock_create_issue, mock_close_issues, mock_get_muted):
        """Тест разбиения больших групп тестов на chunks"""
        from create_new_muted_ya import create_mute_issues
        
        # Создаем много тестов одной команды (больше 40)
        many_tests = []
        for i in range(45):
            many_tests.append({
                'full_name': f'ydb/tests/functional/tpc/large/test{i}',
                'suite_folder': 'ydb/tests/functional/tpc/large',
                'test_name': f'test{i}',
                'owner': 'team/topics',
                'success_rate': 45.5,
                'days_in_state': 5,
                'state': 'Flaky',
                'date_window': 19723,
                'summary': f'Test summary {i}',
                'fail_count': 15,
                'pass_count': 10,
                'branch': 'main'
            })
        
        mock_read_tests.return_value = [
            {'testsuite': 'ydb/tests/functional/tpc/large', 'testcase': f'test{i}', 'full_name': f'ydb/tests/functional/tpc/large/test{i}'}
            for i in range(45)
        ]
        
        mock_get_muted.return_value = {}
        mock_close_issues.return_value = ([], [])
        mock_generate.return_value = ('Test Issue Title', 'Test Issue Body')
        mock_create_issue.return_value = {'issue_url': 'https://github.com/test/issue/1'}
        
        create_mute_issues(many_tests, '/tmp/test_file.txt', close_issues=True)
        
        # Проверяем что создано 2 issue (45 тестов разбиты на 2 группы по 40 и 5)
        self.assertEqual(mock_create_issue.call_count, 2)

    @patch('create_new_muted_ya.get_muted_tests_from_issues')
    @patch('create_new_muted_ya.close_unmuted_issues')
    @patch('create_new_muted_ya.create_and_add_issue_to_project')
    @patch('create_new_muted_ya.generate_github_issue_title_and_body')
    @patch('create_new_muted_ya.read_tests_from_file')
    def test_create_mute_issues_with_closed_issues(self, mock_read_tests, mock_generate, mock_create_issue, mock_close_issues, mock_get_muted):
        """Тест форматирования вывода с закрытыми issues"""
        from create_new_muted_ya import create_mute_issues
        
        mock_read_tests.return_value = []
        mock_get_muted.return_value = {}
        
        # Мокируем закрытые и частично размьюченные issues
        mock_close_issues.return_value = (
            [{'url': 'https://github.com/test/closed/1', 'tests': ['test1', 'test2']}],
            [{'url': 'https://github.com/test/partial/1', 'unmuted_tests': ['test3'], 'still_muted_tests': ['test4']}]
        )
        
        mock_create_issue.return_value = {'issue_url': 'https://github.com/test/issue/1'}
        
        # Перехватываем print для проверки вывода
        with patch('builtins.print') as mock_print:
            create_mute_issues([], '/tmp/test_file.txt', close_issues=True)
        
        # Проверяем что вывод содержит секции для закрытых и частично размьюченных issues
        print_calls = [str(call) for call in mock_print.call_args_list]
        output = ' '.join(print_calls)
        
        self.assertIn('CLOSED ISSUES', output)
        self.assertIn('PARTIALLY UNMUTED ISSUES', output)

    def test_read_tests_from_file(self):
        """Тест чтения тестов из файла"""
        from create_new_muted_ya import read_tests_from_file
        
        test_content = """ydb/tests/functional/tpc/large test1
ydb/core/tx/schemeshard test2
invalid_line_without_space
ydb/library/actors test3"""
        
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write(test_content)
            test_file_path = f.name
        
        try:
            with patch('create_new_muted_ya.logging.warning') as mock_warning:
                result = read_tests_from_file(test_file_path)
            
            # Проверяем корректные результаты
            self.assertEqual(len(result), 3)
            self.assertEqual(result[0]['testsuite'], 'ydb/tests/functional/tpc/large')
            self.assertEqual(result[0]['testcase'], 'test1')
            self.assertEqual(result[0]['full_name'], 'ydb/tests/functional/tpc/large/test1')
            
            # Проверяем что было предупреждение о некорректной строке
            mock_warning.assert_called()
            
        finally:
            os.unlink(test_file_path)


    @patch('create_new_muted_ya.get_muted_tests_from_issues')
    @patch('create_new_muted_ya.close_unmuted_issues')
    @patch('create_new_muted_ya.create_and_add_issue_to_project')
    @patch('create_new_muted_ya.generate_github_issue_title_and_body')
    @patch('create_new_muted_ya.read_tests_from_file')
    def test_create_mute_issues_with_github_output(self, mock_read_tests, mock_generate, mock_create_issue, mock_close_issues, mock_get_muted):
        """Тест записи результатов в GITHUB_OUTPUT"""
        from create_new_muted_ya import create_mute_issues
        
        mock_read_tests.return_value = [
            {'testsuite': 'ydb/tests/functional/tpc/large', 'testcase': 'test1', 'full_name': 'ydb/tests/functional/tpc/large/test1'}
        ]
        
        mock_get_muted.return_value = {}
        mock_close_issues.return_value = ([], [])
        mock_generate.return_value = ('Test Issue Title', 'Test Issue Body')
        mock_create_issue.return_value = {'issue_url': 'https://github.com/test/issue/1'}
        
        # Мокируем переменные окружения
        with patch.dict(os.environ, {'GITHUB_OUTPUT': '/tmp/github_output', 'GITHUB_WORKSPACE': '/tmp/workspace'}):
            with patch('builtins.open', mock_open()) as mock_file:
                create_mute_issues(self.sample_tests_data, '/tmp/test_file.txt', close_issues=True)
                
                # Проверяем что файлы были открыты для записи
                mock_file.assert_called()


if __name__ == '__main__':
    unittest.main(verbosity=2) 