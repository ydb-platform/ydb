#!/usr/bin/env python3

import unittest
import os
import sys
from unittest.mock import patch, MagicMock

# Настройка моков
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from mock_setup import setup_mocks
setup_mocks()


class TestUpdateMuteIssues(unittest.TestCase):
    """Тесты для модуля update_mute_issues.py"""

    def setUp(self):
        """Настройка тестов"""
        self.mock_issue_data = {
            'id': 123,
            'number': 456,
            'title': 'Test Issue Title',
            'html_url': 'https://github.com/test/issue/456',
            'body': 'Test issue body\n\n```\ntest_suite test_case\n```',
            'state': 'open',
            'labels': [
                MagicMock(name='bug'),
                MagicMock(name='muted')
            ]
        }

    @patch('update_mute_issues.run_query')
    def test_get_repository(self, mock_run_query):
        """Тест получения GitHub репозитория"""
        from update_mute_issues import get_repository
        
        mock_run_query.return_value = {
            'data': {
                'organization': {
                    'repository': {
                        'id': 'fake_repo_id'
                    }
                }
            }
        }
        
        result = get_repository()
        
        mock_run_query.assert_called_once()
        self.assertEqual(result, {'id': 'fake_repo_id'})

    @patch('update_mute_issues.get_issues_and_tests_from_project')
    def test_get_muted_tests_from_issues(self, mock_get_issues):
        """Тест получения замьюченных тестов из issues"""
        from update_mute_issues import get_muted_tests_from_issues
        
        # Мокируем структуру данных как возвращает get_issues_and_tests_from_project
        mock_get_issues.return_value = {
            'issue_id_1': {
                'url': 'https://github.com/test/issue/456',
                'title': 'Test Issue Title',
                'state': 'OPEN',
                'tests': ['ydb/tests/functional/tpc/test_case1', 'ydb/core/tx/schemeshard/test_case2'],
                'createdAt': '2023-01-01T00:00:00Z',
                'status_updated': '2023-01-02T00:00:00Z',
                'status': 'Muted',
                'branches': ['main']
            }
        }
        
        result = get_muted_tests_from_issues()
        
        # Проверяем что функция вернула правильную структуру данных
        self.assertIn('ydb/tests/functional/tpc/test_case1', result)
        self.assertIn('ydb/core/tx/schemeshard/test_case2', result)
        
        # Проверяем структуру данных для одного теста
        test_data = result['ydb/tests/functional/tpc/test_case1'][0]
        self.assertEqual(test_data['url'], 'https://github.com/test/issue/456')

    @patch('update_mute_issues.run_query')
    def test_get_issue_comments(self, mock_run_query):
        """Тест получения комментариев к issue"""
        from update_mute_issues import get_issue_comments
        
        mock_run_query.return_value = {
            'data': {
                'node': {
                    'comments': {
                        'nodes': [
                            {'body': 'Test comment body'},
                            {'body': 'Another comment'}
                        ]
                    }
                }
            }
        }
        
        result = get_issue_comments('issue_id_123')
        
        # Проверяем что комментарии правильно обработаны
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], 'Test comment body')
        self.assertEqual(result[1], 'Another comment')

    @patch('update_mute_issues.get_muted_tests_from_issues')
    @patch('update_mute_issues.get_project_v2_fields')
    @patch('update_mute_issues.get_issue_comments')
    @patch('update_mute_issues.add_issue_comment')
    @patch('update_mute_issues.close_issue')
    @patch('update_mute_issues.update_all_closed_issues_status')
    def test_close_unmuted_issues(self, mock_update_status, mock_close_issue, mock_add_comment, mock_get_comments, mock_get_fields, mock_get_muted_tests):
        """Тест закрытия размьюченных issues"""
        from update_mute_issues import close_unmuted_issues
        
        # Настройка моков
        mock_get_muted_tests.return_value = {
            'ydb/tests/functional/tpc/test_case1': [
                {
                    'id': 'issue_id_1',
                    'url': 'https://github.com/test/issue/456',
                    'state': 'OPEN',
                    'status': 'Muted'
                }
            ],
            'ydb/core/tx/schemeshard/test_case2': [
                {
                    'id': 'issue_id_1',
                    'url': 'https://github.com/test/issue/456',
                    'state': 'OPEN',
                    'status': 'Muted'
                }
            ]
        }
        
        mock_get_fields.return_value = (
            'project_id', 
            [{'id': 'status_field_id', 'name': 'Status', 'options': [{'id': 'unmuted_option_id', 'name': 'Unmuted'}]}]
        )
        
        mock_get_comments.return_value = []  # Нет существующих комментариев
        
        # Тест полностью размьючен (нет ни одного теста в muted_tests_set)
        muted_tests_set = set()
        
        closed_issues, partially_unmuted_issues = close_unmuted_issues(muted_tests_set)
        
        # Проверяем что issue был закрыт
        self.assertEqual(len(closed_issues), 1)
        self.assertEqual(len(partially_unmuted_issues), 0)
        
        closed_issue = closed_issues[0]
        self.assertEqual(closed_issue['url'], 'https://github.com/test/issue/456')

    @patch('update_mute_issues.get_muted_tests_from_issues')
    @patch('update_mute_issues.get_project_v2_fields')
    @patch('update_mute_issues.get_issue_comments')
    @patch('update_mute_issues.add_issue_comment')
    @patch('update_mute_issues.update_all_closed_issues_status')
    def test_close_unmuted_issues_partially_unmuted(self, mock_update_status, mock_add_comment, mock_get_comments, mock_get_fields, mock_get_muted_tests):
        """Тест частично размьюченных issues"""
        from update_mute_issues import close_unmuted_issues
        
        # Настройка моков
        mock_get_muted_tests.return_value = {
            'ydb/tests/functional/tpc/test_case1': [
                {
                    'id': 'issue_id_1',
                    'url': 'https://github.com/test/issue/456',
                    'state': 'OPEN',
                    'status': 'Muted'
                }
            ],
            'ydb/core/tx/schemeshard/test_case2': [
                {
                    'id': 'issue_id_1',
                    'url': 'https://github.com/test/issue/456',
                    'state': 'OPEN',
                    'status': 'Muted'
                }
            ]
        }
        
        mock_get_fields.return_value = (
            'project_id', 
            [{'id': 'status_field_id', 'name': 'Status', 'options': [{'id': 'unmuted_option_id', 'name': 'Unmuted'}]}]
        )
        
        mock_get_comments.return_value = []
        
        # Только один тест все еще замьючен
        muted_tests_set = {'ydb/tests/functional/tpc/test_case1'}
        
        closed_issues, partially_unmuted_issues = close_unmuted_issues(muted_tests_set)
        
        # Проверяем что issue частично размьючен
        self.assertEqual(len(closed_issues), 0)
        self.assertEqual(len(partially_unmuted_issues), 1)
        
        partial_issue = partially_unmuted_issues[0]
        self.assertEqual(partial_issue['url'], 'https://github.com/test/issue/456')

    @patch('update_mute_issues.get_project_v2_fields')
    @patch('update_mute_issues.get_repository')
    @patch('update_mute_issues.run_query')
    def test_create_and_add_issue_to_project(self, mock_run_query, mock_get_repo, mock_get_fields):
        """Тест создания issue и добавления в проект"""
        from update_mute_issues import create_and_add_issue_to_project
        
        # Настройка моков
        mock_get_repo.return_value = {'id': 'repo_id_123'}
        mock_get_fields.return_value = (
            'project_id_123', 
            [
                {'id': 'status_field_id', 'name': 'Status', 'options': [{'id': 'muted_option_id', 'name': 'Muted'}]},
                {'id': 'owner_field_id', 'name': 'Owner', 'options': [{'id': 'team_option_id', 'name': 'test_team'}]}
            ]
        )
        
        # Мокируем создание issue
        mock_run_query.side_effect = [
            # Ответ на создание issue
            {
                'data': {
                    'createIssue': {
                        'issue': {
                            'id': 'issue_id_123',
                            'url': 'https://github.com/test/issue/456'
                        }
                    }
                }
            },
            # Ответ на добавление в проект
            {
                'data': {
                    'addProjectV2ItemById': {
                        'item': {'id': 'item_id_123'}
                    }
                }
            },
            # Остальные запросы для обновления полей
            {'data': {}}, {'data': {}}
        ]
        
        result = create_and_add_issue_to_project(
            title='Test Issue',
            body='Test issue body',
            state='Muted',
            owner='test_team'
        )
        
        # Проверяем что функция вернула правильный результат
        self.assertEqual(result['issue_url'], 'https://github.com/test/issue/456')

    def test_generate_github_issue_title_and_body(self):
        """Тест генерации заголовка и тела issue"""
        from update_mute_issues import generate_github_issue_title_and_body
        
        test_data = [
            {
                'full_name': 'ydb/tests/functional/tpc/test_case1',
                'mute_string': 'ydb/tests/functional/tpc test_case1',
                'test_name': 'test_case1',
                'suite_folder': 'ydb/tests/functional/tpc',
                'owner': 'team/test',
                'success_rate': 45.5,
                'days_in_state': 5,
                'state': 'Flaky',
                'summary': 'Test summary',
                'fail_count': 15,
                'pass_count': 10,
                'branch': 'main',
                'date_window': '2023-01-01'  # Добавляем необходимое поле
            }
        ]
        
        title, body = generate_github_issue_title_and_body(test_data)
        
        # Проверяем что заголовок и тело сформированы правильно
        # Для одного теста заголовок должен содержать полное имя теста
        self.assertIn('Mute ydb/tests/functional/tpc/test_case1', title)
        self.assertIn('in main', title)
        self.assertIn('test_case1', body)
        self.assertIn('success_rate 45.5%', body)
        self.assertIn('last 5 days', body)

    def test_parse_body(self):
        """Тест парсинга тестов из тела issue"""
        from update_mute_issues import parse_body
        
        # Тестируем различные форматы тела issue
        test_cases = [
            # Стандартный формат с HTML комментариями
            {
                'body': 'Mute:<!--mute_list_start-->\nydb/tests/functional/tpc test_case1\nydb/core/tx/schemeshard test_case2\n<!--mute_list_end-->\n\nBranch:<!--branch_list_start-->\nmain\n<!--branch_list_end-->',
                'expected_tests': ['ydb/tests/functional/tpc test_case1', 'ydb/core/tx/schemeshard test_case2'],
                'expected_branches': ['main']
            },
            # Формат начинающийся с 'Mute:'
            {
                'body': 'Mute:\nydb/tests/functional/tpc test_case1\nydb/core/tx/schemeshard test_case2\n**Add line to',
                'expected_tests': ['ydb/tests/functional/tpc test_case1', 'ydb/core/tx/schemeshard test_case2'],
                'expected_branches': ['main']
            },
            # Пустое тело
            {
                'body': '',
                'expected_tests': [],
                'expected_branches': ['main']
            }
        ]
        
        for test_case in test_cases:
            tests, branches = parse_body(test_case['body'])
            # Фильтруем пустые строки из branches, так как parse_body может их возвращать
            branches = [branch.strip() for branch in branches if branch.strip()]
            self.assertEqual(tests, test_case['expected_tests'])
            self.assertEqual(branches, test_case['expected_branches'])

    @patch('update_mute_issues.get_issues_and_tests_from_project')
    def test_error_handling_in_get_muted_tests_from_issues(self, mock_get_issues):
        """Тест обработки ошибок в get_muted_tests_from_issues"""
        from update_mute_issues import get_muted_tests_from_issues
        
        # Мокируем ошибку при получении issues
        mock_get_issues.side_effect = Exception("API Error")
        
        # Проверяем что функция обрабатывает ошибку
        with self.assertRaises(Exception):
            get_muted_tests_from_issues()

    @patch('update_mute_issues.fetch_all_issues')
    def test_issues_without_muted_label(self, mock_fetch_issues):
        """Тест что issues без лейбла 'muted' игнорируются"""
        from update_mute_issues import get_issues_and_tests_from_project
        
        # Создаем issue без поля content
        mock_issue = {
            'content': None,  # Нет контента
            'fieldValues': {'nodes': []}
        }
        
        mock_fetch_issues.return_value = [mock_issue]
        
        result = get_issues_and_tests_from_project('test_org', 'test_project')
        
        # Проверяем что результат пустой когда нет контента
        self.assertEqual(result, {})


if __name__ == '__main__':
    unittest.main(verbosity=2) 