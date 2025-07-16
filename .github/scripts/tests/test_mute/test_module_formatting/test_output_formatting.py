#!/usr/bin/env python3

import unittest
import os
import sys
from unittest.mock import patch, MagicMock

# Настройка моков
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from mock_setup import setup_mocks
setup_mocks()


class TestOutputFormatting(unittest.TestCase):
    """Тесты для логики форматирования вывода"""

    def setUp(self):
        """Настройка тестов"""
        self.sample_results = [
            {
                'message': "Created issue 'Test Issue 1' for TEAM:@ydb-platform/team1, url https://github.com/test/issue/1",
                'owner': 'team1'
            },
            {
                'message': "Created issue 'Test Issue 2' for TEAM:@ydb-platform/team1, url https://github.com/test/issue/2",
                'owner': 'team1'
            },
            {
                'message': "Created issue 'Test Issue 3' for TEAM:@ydb-platform/team2, url https://github.com/test/issue/3",
                'owner': 'team2'
            }
        ]

    def test_formatting_closed_issues_only(self):
        """Тест форматирования только закрытых issues"""
        from create_new_muted_ya import create_mute_issues
        
        closed_issues = [
            {
                'url': 'https://github.com/test/closed/1',
                'tests': ['ydb/tests/functional/tpc/test1', 'ydb/core/tx/schemeshard/test2']
            }
        ]
        
        # Мокируем все зависимости
        with patch('create_new_muted_ya.read_tests_from_file') as mock_read_tests, \
             patch('create_new_muted_ya.get_muted_tests_from_issues') as mock_get_muted, \
             patch('create_new_muted_ya.close_unmuted_issues') as mock_close_issues, \
             patch('builtins.print') as mock_print:
            
            mock_read_tests.return_value = []
            mock_get_muted.return_value = {}
            mock_close_issues.return_value = (closed_issues, [])
            
            create_mute_issues([], '/tmp/test.txt', close_issues=True)
            
            # Собираем весь вывод
            print_calls = [str(call) for call in mock_print.call_args_list]
            output = ' '.join(print_calls)
            
            # Проверяем содержимое
            self.assertIn('🔒 **CLOSED ISSUES**', output)
            self.assertIn('✅ **Closed**', output)
            self.assertIn('https://github.com/test/closed/1', output)
            self.assertIn('📝 **Unmuted tests:**', output)
            self.assertIn('ydb/tests/functional/tpc/test1', output)
            self.assertIn('ydb/core/tx/schemeshard/test2', output)

    def test_formatting_partially_unmuted_issues_only(self):
        """Тест форматирования только частично размьюченных issues"""
        from create_new_muted_ya import create_mute_issues
        
        partially_unmuted_issues = [
            {
                'url': 'https://github.com/test/partial/1',
                'unmuted_tests': ['ydb/tests/functional/tpc/test1'],
                'still_muted_tests': ['ydb/core/tx/schemeshard/test2']
            }
        ]
        
        # Мокируем все зависимости
        with patch('create_new_muted_ya.read_tests_from_file') as mock_read_tests, \
             patch('create_new_muted_ya.get_muted_tests_from_issues') as mock_get_muted, \
             patch('create_new_muted_ya.close_unmuted_issues') as mock_close_issues, \
             patch('builtins.print') as mock_print:
            
            mock_read_tests.return_value = []
            mock_get_muted.return_value = {}
            mock_close_issues.return_value = ([], partially_unmuted_issues)
            
            create_mute_issues([], '/tmp/test.txt', close_issues=True)
            
            # Собираем весь вывод
            print_calls = [str(call) for call in mock_print.call_args_list]
            output = ' '.join(print_calls)
            
            # Проверяем содержимое
            self.assertIn('🔓 **PARTIALLY UNMUTED ISSUES**', output)
            self.assertIn('⚠️ **Partially unmuted**', output)
            self.assertIn('https://github.com/test/partial/1', output)
            self.assertIn('📝 **Unmuted tests:**', output)
            self.assertIn('ydb/tests/functional/tpc/test1', output)
            self.assertIn('🔒 **Still muted tests:**', output)
            self.assertIn('ydb/core/tx/schemeshard/test2', output)

    def test_formatting_created_issues_only(self):
        """Тест форматирования только созданных issues"""
        from create_new_muted_ya import create_mute_issues
        
        # Мокируем все зависимости
        with patch('create_new_muted_ya.read_tests_from_file') as mock_read_tests, \
             patch('create_new_muted_ya.get_muted_tests_from_issues') as mock_get_muted, \
             patch('create_new_muted_ya.close_unmuted_issues') as mock_close_issues, \
             patch('create_new_muted_ya.create_and_add_issue_to_project') as mock_create_issue, \
             patch('create_new_muted_ya.generate_github_issue_title_and_body') as mock_generate, \
             patch('builtins.print') as mock_print:
            
            mock_read_tests.return_value = [
                {'testsuite': 'ydb/tests/functional/tpc', 'testcase': 'test1', 'full_name': 'ydb/tests/functional/tpc/test1'}
            ]
            mock_get_muted.return_value = {}
            mock_close_issues.return_value = ([], [])
            mock_generate.return_value = ('Test Issue Title', 'Test Issue Body')
            mock_create_issue.return_value = {'issue_url': 'https://github.com/test/issue/1'}
            
            # Пример данных с тестами
            test_data = [
                {
                    'full_name': 'ydb/tests/functional/tpc/test1',
                    'suite_folder': 'ydb/tests/functional/tpc',
                    'test_name': 'test1',
                    'owner': 'team/test',
                    'success_rate': 45.5,
                    'days_in_state': 5,
                    'state': 'Flaky',
                    'date_window': 19723,
                    'summary': 'Test summary',
                    'fail_count': 15,
                    'pass_count': 10,
                    'branch': 'main'
                }
            ]
            
            create_mute_issues(test_data, '/tmp/test.txt', close_issues=True)
            
            # Собираем весь вывод
            print_calls = [str(call) for call in mock_print.call_args_list]
            output = ' '.join(print_calls)
            
            # Проверяем содержимое
            self.assertIn('🆕 **CREATED ISSUES**', output)
            self.assertIn('👥 **TEAM** @ydb-platform/test', output)
            self.assertIn('https://github.com/orgs/ydb-platform/teams/test', output)
            self.assertIn('🎯 https://github.com/test/issue/1', output)

    def test_formatting_all_sections(self):
        """Тест форматирования всех секций одновременно"""
        from create_new_muted_ya import create_mute_issues
        
        closed_issues = [
            {
                'url': 'https://github.com/test/closed/1',
                'tests': ['ydb/tests/functional/tpc/test1']
            }
        ]
        
        partially_unmuted_issues = [
            {
                'url': 'https://github.com/test/partial/1',
                'unmuted_tests': ['ydb/tests/functional/tpc/test2'],
                'still_muted_tests': ['ydb/core/tx/schemeshard/test3']
            }
        ]
        
        # Мокируем все зависимости
        with patch('create_new_muted_ya.read_tests_from_file') as mock_read_tests, \
             patch('create_new_muted_ya.get_muted_tests_from_issues') as mock_get_muted, \
             patch('create_new_muted_ya.close_unmuted_issues') as mock_close_issues, \
             patch('create_new_muted_ya.create_and_add_issue_to_project') as mock_create_issue, \
             patch('create_new_muted_ya.generate_github_issue_title_and_body') as mock_generate, \
             patch('builtins.print') as mock_print:
            
            mock_read_tests.return_value = [
                {'testsuite': 'ydb/tests/functional/tpc', 'testcase': 'test4', 'full_name': 'ydb/tests/functional/tpc/test4'}
            ]
            mock_get_muted.return_value = {}
            mock_close_issues.return_value = (closed_issues, partially_unmuted_issues)
            mock_generate.return_value = ('Test Issue Title', 'Test Issue Body')
            mock_create_issue.return_value = {'issue_url': 'https://github.com/test/issue/1'}
            
            # Пример данных с тестами
            test_data = [
                {
                    'full_name': 'ydb/tests/functional/tpc/test4',
                    'suite_folder': 'ydb/tests/functional/tpc',
                    'test_name': 'test4',
                    'owner': 'team/test',
                    'success_rate': 45.5,
                    'days_in_state': 5,
                    'state': 'Flaky',
                    'date_window': 19723,
                    'summary': 'Test summary',
                    'fail_count': 15,
                    'pass_count': 10,
                    'branch': 'main'
                }
            ]
            
            create_mute_issues(test_data, '/tmp/test.txt', close_issues=True)
            
            # Собираем весь вывод
            print_calls = [str(call) for call in mock_print.call_args_list]
            output = ' '.join(print_calls)
            
            # Проверяем что все секции присутствуют
            self.assertIn('🔒 **CLOSED ISSUES**', output)
            self.assertIn('🔓 **PARTIALLY UNMUTED ISSUES**', output)
            self.assertIn('🆕 **CREATED ISSUES**', output)
            
            # Проверяем разделители
            self.assertIn('─────────────────────────────', output)

    def test_team_grouping_and_sorting(self):
        """Тест группировки и сортировки по командам"""
        from create_new_muted_ya import create_mute_issues
        
        # Мокируем все зависимости
        with patch('create_new_muted_ya.read_tests_from_file') as mock_read_tests, \
             patch('create_new_muted_ya.get_muted_tests_from_issues') as mock_get_muted, \
             patch('create_new_muted_ya.close_unmuted_issues') as mock_close_issues, \
             patch('create_new_muted_ya.create_and_add_issue_to_project') as mock_create_issue, \
             patch('create_new_muted_ya.generate_github_issue_title_and_body') as mock_generate, \
             patch('builtins.print') as mock_print:
            
            mock_read_tests.return_value = [
                {'testsuite': 'ydb/tests/functional/tpc', 'testcase': 'test1', 'full_name': 'ydb/tests/functional/tpc/test1'},
                {'testsuite': 'ydb/core/tx/schemeshard', 'testcase': 'test2', 'full_name': 'ydb/core/tx/schemeshard/test2'}
            ]
            mock_get_muted.return_value = {}
            mock_close_issues.return_value = ([], [])
            mock_generate.return_value = ('Test Issue Title', 'Test Issue Body')
            
            # Мокируем создание разных issues для разных команд
            def create_issue_side_effect(title, body, state, owner):
                return {'issue_url': f'https://github.com/test/issue/{owner}'}
            
            mock_create_issue.side_effect = create_issue_side_effect
            
            # Пример данных с тестами из разных команд
            test_data = [
                {
                    'full_name': 'ydb/tests/functional/tpc/test1',
                    'suite_folder': 'ydb/tests/functional/tpc',
                    'test_name': 'test1',
                    'owner': 'team/zteam',  # Команда которая должна быть последней в алфавитном порядке
                    'success_rate': 45.5,
                    'days_in_state': 5,
                    'state': 'Flaky',
                    'date_window': 19723,
                    'summary': 'Test summary',
                    'fail_count': 15,
                    'pass_count': 10,
                    'branch': 'main'
                },
                {
                    'full_name': 'ydb/core/tx/schemeshard/test2',
                    'suite_folder': 'ydb/core/tx/schemeshard',
                    'test_name': 'test2',
                    'owner': 'team/ateam',  # Команда которая должна быть первой в алфавитном порядке
                    'success_rate': 30.0,
                    'days_in_state': 8,
                    'state': 'Flaky',
                    'date_window': 19723,
                    'summary': 'Another test summary',
                    'fail_count': 20,
                    'pass_count': 5,
                    'branch': 'main'
                }
            ]
            
            create_mute_issues(test_data, '/tmp/test.txt', close_issues=True)
            
            # Собираем весь вывод
            print_calls = [str(call) for call in mock_print.call_args_list]
            output = ' '.join(print_calls)
            
            # Проверяем что команды отсортированы по алфавиту
            ateam_pos = output.find('ateam')
            zteam_pos = output.find('zteam')
            self.assertLess(ateam_pos, zteam_pos, "Команды должны быть отсортированы по алфавиту")
            
            # Проверяем что есть заголовки команд
            self.assertIn('👥 **TEAM** @ydb-platform/ateam', output)
            self.assertIn('👥 **TEAM** @ydb-platform/zteam', output)
            
            # Проверяем что есть ссылки на команды
            self.assertIn('https://github.com/orgs/ydb-platform/teams/ateam', output)
            self.assertIn('https://github.com/orgs/ydb-platform/teams/zteam', output)

    def test_team_spacing_between_groups(self):
        """Тест двойного отступа между группами команд"""
        from create_new_muted_ya import create_mute_issues
        
        # Мокируем все зависимости
        with patch('create_new_muted_ya.read_tests_from_file') as mock_read_tests, \
             patch('create_new_muted_ya.get_muted_tests_from_issues') as mock_get_muted, \
             patch('create_new_muted_ya.close_unmuted_issues') as mock_close_issues, \
             patch('create_new_muted_ya.create_and_add_issue_to_project') as mock_create_issue, \
             patch('create_new_muted_ya.generate_github_issue_title_and_body') as mock_generate, \
             patch('builtins.print') as mock_print:
            
            mock_read_tests.return_value = [
                {'testsuite': 'ydb/tests/functional/tpc', 'testcase': 'test1', 'full_name': 'ydb/tests/functional/tpc/test1'},
                {'testsuite': 'ydb/core/tx/schemeshard', 'testcase': 'test2', 'full_name': 'ydb/core/tx/schemeshard/test2'}
            ]
            mock_get_muted.return_value = {}
            mock_close_issues.return_value = ([], [])
            mock_generate.return_value = ('Test Issue Title', 'Test Issue Body')
            
            # Мокируем создание разных issues для разных команд
            def create_issue_side_effect(title, body, state, owner):
                return {'issue_url': f'https://github.com/test/issue/{owner}'}
            
            mock_create_issue.side_effect = create_issue_side_effect
            
            # Пример данных с тестами из разных команд
            test_data = [
                {
                    'full_name': 'ydb/tests/functional/tpc/test1',
                    'suite_folder': 'ydb/tests/functional/tpc',
                    'test_name': 'test1',
                    'owner': 'team/team1',
                    'success_rate': 45.5,
                    'days_in_state': 5,
                    'state': 'Flaky',
                    'date_window': 19723,
                    'summary': 'Test summary',
                    'fail_count': 15,
                    'pass_count': 10,
                    'branch': 'main'
                },
                {
                    'full_name': 'ydb/core/tx/schemeshard/test2',
                    'suite_folder': 'ydb/core/tx/schemeshard',
                    'test_name': 'test2',
                    'owner': 'team/team2',
                    'success_rate': 30.0,
                    'days_in_state': 8,
                    'state': 'Flaky',
                    'date_window': 19723,
                    'summary': 'Another test summary',
                    'fail_count': 20,
                    'pass_count': 5,
                    'branch': 'main'
                }
            ]
            
            create_mute_issues(test_data, '/tmp/test.txt', close_issues=True)
            
            # Собираем весь вывод
            print_calls = [str(call) for call in mock_print.call_args_list]
            output = ' '.join(print_calls)
            
            # Проверяем что между командами есть двойной отступ
            # Ищем паттерн где между командами есть пустые строки
            lines = []
            for call in mock_print.call_args_list:
                if call[0]:  # если есть аргументы
                    if isinstance(call[0][0], str) and '\n' in call[0][0]:
                        lines.extend(call[0][0].split('\n'))
                    else:
                        lines.append(str(call[0][0]))
            
            # Проверяем что есть последовательность: команда -> пустая строка -> пустая строка -> команда  
            team1_found = False
            team2_found = False
            double_empty_found = False
            
            for i, line in enumerate(lines):
                if 'team1' in line:
                    team1_found = True
                elif team1_found and line == '' and i+1 < len(lines) and lines[i+1] == '':
                    double_empty_found = True
                elif double_empty_found and 'team2' in line:
                    team2_found = True
                    break
            
            self.assertTrue(double_empty_found, "Должны быть двойные пустые строки между командами")


if __name__ == '__main__':
    unittest.main(verbosity=2) 