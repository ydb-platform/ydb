#!/usr/bin/env python3
"""
Консолидированные тесты для create_new_muted_ya.py
Содержат только самые важные тесты, которые реально проверяют логику.
"""

import unittest
import os
import tempfile
import shutil
from unittest.mock import Mock, patch, call

# Настройка моков
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from mock_setup import setup_mocks
setup_mocks()

from create_new_muted_ya import apply_and_add_mutes


class TestCreateNewMutedYaConsolidated(unittest.TestCase):
    """Консолидированные тесты для основной логики мьютинга"""
    
    def setUp(self):
        """Setup for each test"""
        self.temp_dir = tempfile.mkdtemp()
        self.mock_mute_check = Mock()
        
    def tearDown(self):
        """Cleanup after each test"""
        shutil.rmtree(self.temp_dir)
    
    def create_test_data(self, **kwargs):
        """Helper to create test data with default values"""
        default_data = {
            'suite_folder': 'test_suite',
            'test_name': 'test_case',
            'full_name': 'test_suite/test_case',
            'success_rate': 50.0,
            'days_in_state': 5,
            'owner': 'team/developer',
            'state': 'Flaky',
            'pass_count': 5,
            'fail_count': 5,
            'skip_count': 0,
            'mute_count': 0,
            'date_window': 19000,
            'summary': 'Test summary',
            'branch': 'main',
            'flaky_today': False,
            'deleted_today': False,
            'muted_stable_n_days_today': False,
            'new_flaky_today': False,
            'muted_stable_today': False,
        }
        default_data.update(kwargs)
        return default_data

    @patch('create_new_muted_ya.add_lines_to_file')
    def test_new_flaky_rules_working(self, mock_add_lines):
        """Тест проверяет новые правила: flaky_today=True AND fail_count >= 2"""
        
        test_data = [
            # Этот тест ДОЛЖЕН попасть в flaky (условия выполнены)
            self.create_test_data(
                test_name='test_3_failures_should_be_muted',
                flaky_today=True,
                fail_count=3,  # >= 2 падения
                pass_count=10,
            ),
            
            # Этот тест НЕ ДОЛЖЕН попасть в flaky (недостаточно падений)
            self.create_test_data(
                test_name='test_1_failure_too_few',
                flaky_today=True,
                fail_count=1,  # < 2 падений - НЕ ХВАТАЕТ!
                pass_count=10,
            ),
        ]
        
        self.mock_mute_check.return_value = False
        
        result = apply_and_add_mutes(test_data, self.temp_dir, self.mock_mute_check)
        
        # Проверяем flaky файл
        flaky_calls = [call for call in mock_add_lines.call_args_list 
                      if 'flaky.txt' in str(call) and 'debug' not in str(call)]
        
        self.assertTrue(flaky_calls, "Файл flaky.txt не был создан")
        
        flaky_content = flaky_calls[0][0][1]
        flaky_text = ' '.join([line.strip() for line in flaky_content if line.strip()])
        
        # Проверяем правила с информативными сообщениями
        self.assertIn('test_3_failures_should_be_muted', flaky_text, 
                     f"ОШИБКА: Тест 'test_3_failures_should_be_muted' (3 падения) должен быть в flaky.txt, но его там нет. "
                     f"Содержимое flaky.txt: {flaky_text}")
        
        self.assertNotIn('test_1_failure_too_few', flaky_text,
                        f"ОШИБКА: Тест 'test_1_failure_too_few' (1 падение) НЕ должен быть в flaky.txt, но он там есть! "
                        f"Это нарушает правило 'fail_count >= 2'. Содержимое flaky.txt: {flaky_text}")

    @patch('create_new_muted_ya.add_lines_to_file')
    def test_deleted_and_stable_exclusion(self, mock_add_lines):
        """Тест проверяет исключение deleted и stable тестов"""
        
        test_data = [
            # Deleted тест - не должен попасть в новые мьюты
            self.create_test_data(
                test_name='deleted_test',
                deleted_today=True,
                flaky_today=True,
                fail_count=10,
            ),
            
            # Stable тест - не должен попасть в новые мьюты
            self.create_test_data(
                test_name='stable_test',
                muted_stable_n_days_today=True,
                flaky_today=True,
                fail_count=10,
            ),
            
            # Нормальный flaky тест
            self.create_test_data(
                test_name='normal_flaky',
                flaky_today=True,
                fail_count=5,
            ),
        ]
        
        self.mock_mute_check.return_value = False
        
        result = apply_and_add_mutes(test_data, self.temp_dir, self.mock_mute_check)
        
        # Проверяем deleted файл
        deleted_calls = [call for call in mock_add_lines.call_args_list 
                        if 'deleted.txt' in str(call) and 'debug' not in str(call)]
        self.assertEqual(len(deleted_calls), 1)
        
        deleted_content = deleted_calls[0][0][1]
        deleted_tests = [line.strip() for line in deleted_content if line.strip()]
        self.assertEqual(len(deleted_tests), 1)
        self.assertIn('deleted_test', deleted_tests[0])
        
        # Проверяем stable файл
        stable_calls = [call for call in mock_add_lines.call_args_list 
                       if 'muted_stable.txt' in str(call) and 'debug' not in str(call)]
        self.assertEqual(len(stable_calls), 1)
        
        stable_content = stable_calls[0][0][1]
        stable_tests = [line.strip() for line in stable_content if line.strip()]
        self.assertEqual(len(stable_tests), 1)
        self.assertIn('stable_test', stable_tests[0])

    @patch('create_new_muted_ya.add_lines_to_file')
    def test_boundary_conditions_fail_count(self, mock_add_lines):
        """Тест граничных условий для fail_count - граница должна быть >= 2"""
        
        test_data = [
            # Точно на границе (2 падения) - ДОЛЖЕН попасть
            self.create_test_data(
                test_name='test_2_failures_exact_boundary',
                flaky_today=True,
                fail_count=2,  # Точно на границе
                pass_count=10,
            ),
            
            # Меньше границы (1 падение) - НЕ ДОЛЖЕН попасть
            self.create_test_data(
                test_name='test_1_failure_below_boundary',
                flaky_today=True,
                fail_count=1,  # Меньше границы - НЕ ХВАТАЕТ!
                pass_count=10,
            ),
            
            # Больше границы (3 падения) - ДОЛЖЕН попасть
            self.create_test_data(
                test_name='test_3_failures_above_boundary',
                flaky_today=True,
                fail_count=3,  # Больше границы
                pass_count=10,
            ),
        ]
        
        self.mock_mute_check.return_value = False
        
        result = apply_and_add_mutes(test_data, self.temp_dir, self.mock_mute_check)
        
        # Проверяем flaky файл
        flaky_calls = [call for call in mock_add_lines.call_args_list 
                      if 'flaky.txt' in str(call) and 'debug' not in str(call)]
        
        self.assertTrue(flaky_calls, "Файл flaky.txt не был создан")
        
        flaky_content = flaky_calls[0][0][1]
        flaky_text = ' '.join([line.strip() for line in flaky_content if line.strip()])
        
        # Проверяем граничные условия с детальными сообщениями
        self.assertIn('test_2_failures_exact_boundary', flaky_text,
                     f"ОШИБКА: Тест 'test_2_failures_exact_boundary' (2 падения) должен быть в flaky.txt (граница >= 2). "
                     f"Содержимое flaky.txt: {flaky_text}")
        
        self.assertNotIn('test_1_failure_below_boundary', flaky_text,
                        f"ОШИБКА: Тест 'test_1_failure_below_boundary' (1 падение) НЕ должен быть в flaky.txt (< 2 падений). "
                        f"Это нарушает правило граничных условий! Содержимое flaky.txt: {flaky_text}")
        
        self.assertIn('test_3_failures_above_boundary', flaky_text,
                     f"ОШИБКА: Тест 'test_3_failures_above_boundary' (3 падения) должен быть в flaky.txt (> 2 падений). "
                     f"Содержимое flaky.txt: {flaky_text}")

    @patch('create_new_muted_ya.add_lines_to_file')
    def test_mute_check_overrides_everything(self, mock_add_lines):
        """Тест что mute_check переопределяет все остальные условия"""
        
        test_data = [
            # Тест который НЕ flaky, но mute_check=True - должен попасть в new_muted_ya
            self.create_test_data(
                test_name='mute_check_override',
                flaky_today=False,
                fail_count=0,
                success_rate=100.0,
            ),
            
            # Тест который flaky, но mute_check=False - не должен попасть в new_muted_ya
            self.create_test_data(
                test_name='flaky_but_not_muted',
                flaky_today=True,
                fail_count=5,
                success_rate=50.0,
            ),
        ]
        
        def mock_side_effect(suite, testcase):
            return testcase == 'mute_check_override'
        
        self.mock_mute_check.side_effect = mock_side_effect
        
        result = apply_and_add_mutes(test_data, self.temp_dir, self.mock_mute_check)
        
        # Проверяем new_muted_ya файл
        new_muted_calls = [call for call in mock_add_lines.call_args_list 
                          if 'new_muted_ya.txt' in str(call) and 'debug' not in str(call)]
        
        new_muted_content = new_muted_calls[0][0][1]
        new_muted_text = ' '.join([line.strip() for line in new_muted_content if line.strip()])
        
        # Проверяем что mute_check переопределяет логику
        self.assertIn('mute_check_override', new_muted_text)
        self.assertNotIn('flaky_but_not_muted', new_muted_text)

    @patch('create_new_muted_ya.add_lines_to_file')
    def test_regex_wildcards_replacement(self, mock_add_lines):
        """Тест замены числовых паттернов на wildcards"""
        
        test_data = [
            self.create_test_data(
                test_name='chunk_test[12/34]',
                suite_folder='test_suite[56/78]',
                flaky_today=True,
                fail_count=3,
            ),
        ]
        
        self.mock_mute_check.return_value = False
        
        result = apply_and_add_mutes(test_data, self.temp_dir, self.mock_mute_check)
        
        # Проверяем flaky файл
        flaky_calls = [call for call in mock_add_lines.call_args_list 
                      if 'flaky.txt' in str(call) and 'debug' not in str(call)]
        
        flaky_content = flaky_calls[0][0][1]
        flaky_text = ' '.join([line.strip() for line in flaky_content if line.strip()])
        
        # Проверяем что числа заменены на wildcards
        self.assertIn('*/*', flaky_text)
        self.assertNotIn('12/34', flaky_text)
        self.assertNotIn('56/78', flaky_text)

    @patch('create_new_muted_ya.add_lines_to_file')
    def test_return_value_correctness(self, mock_add_lines):
        """Тест правильности возвращаемого значения"""
        
        test_data = [
            # 2 теста которые должны попасть в new_muted_ya (через mute_check=True)
            self.create_test_data(
                test_name='test1',
                suite_folder='suite1',
                flaky_today=False,  # Не важно для mute_check
                fail_count=0,       # Не важно для mute_check
            ),
            self.create_test_data(
                test_name='test2', 
                suite_folder='suite2',
                flaky_today=False,  # Не важно для mute_check
                fail_count=0,       # Не важно для mute_check
            ),
            
            # 1 тест который НЕ должен попасть (mute_check=False)
            self.create_test_data(
                test_name='test3',
                suite_folder='suite3',
                flaky_today=True,
                fail_count=10,
            ),
        ]
        
        # Настраиваем mute_check чтобы первые 2 теста попали в new_muted_ya
        def mock_side_effect(suite, testcase):
            return testcase in ['test1', 'test2']
        
        self.mock_mute_check.side_effect = mock_side_effect
        
        result = apply_and_add_mutes(test_data, self.temp_dir, self.mock_mute_check)
        
        # Возвращаемое значение должно быть количеством тестов в new_muted_ya (только те где mute_check=True)
        self.assertEqual(result, 2)

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
            {'testsuite': 'test_suite', 'testcase': 'test1', 'full_name': 'test_suite/test1'},
            {'testsuite': 'test_suite', 'testcase': 'test2', 'full_name': 'test_suite/test2'}
        ]
        
        mock_get_muted.return_value = {}  # Нет уже существующих issue
        mock_close_issues.return_value = ([], [])  # Нет закрытых issues
        mock_generate.return_value = ('Test Issue Title', 'Test Issue Body')
        mock_create_issue.return_value = {'issue_url': 'https://github.com/test/issue/1'}
        
        test_data = [
            self.create_test_data(
                test_name='test1',
                suite_folder='test_suite',
                full_name='test_suite/test1',
                owner='team/test',
            ),
            self.create_test_data(
                test_name='test2',
                suite_folder='test_suite',
                full_name='test_suite/test2',
                owner='team/test',
            ),
        ]
        
        # Тест
        create_mute_issues(test_data, '/tmp/test_file.txt', close_issues=True)
        
        # Проверки
        mock_read_tests.assert_called_once_with('/tmp/test_file.txt')
        mock_get_muted.assert_called_once()
        mock_close_issues.assert_called_once()
        mock_generate.assert_called()
        mock_create_issue.assert_called()

    @patch('create_new_muted_ya.logging.error')
    def test_error_handling(self, mock_error):
        """Тест обработки ошибок"""
        
        # Данные с отсутствующими ключами
        invalid_data = [
            {
                'full_name': 'test/invalid',
                'suite_folder': 'test',
                'test_name': 'invalid',
                'owner': 'team/test',
                'flaky_today': True,
                # Отсутствует fail_count
            }
        ]
        
        result = apply_and_add_mutes(invalid_data, self.temp_dir, self.mock_mute_check)
        
        # Проверяем что ошибка была залогирована
        mock_error.assert_called()
        # Функция должна вернуть 0 при ошибке
        self.assertEqual(result, 0)


if __name__ == '__main__':
    unittest.main() 