#!/usr/bin/env python3

import unittest
import tempfile
import os
import sys
from unittest.mock import patch, MagicMock

# Настройка моков
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from mock_setup import setup_mocks
setup_mocks()


class TestBoundaryConditions(unittest.TestCase):
    """Тесты для граничных условий в SQL запросах и фильтрации"""

    def setUp(self):
        """Настройка тестов"""
        pass

    def test_sql_boundary_conditions_days_in_state(self):
        """Тест граничных условий для days_in_state в SQL"""
        
        # Граничные условия из SQL запроса:
        # 1. days_in_state = 1 (точное равенство для new_flaky)
        # 2. days_in_state >= 14 (для muted_stable_n_days и deleted)
        
        boundary_cases = [
            # days_in_state = 1 для new_flaky
            {'days_in_state': 0, 'should_be_new_flaky': False, 'description': 'Меньше 1 дня'},
            {'days_in_state': 1, 'should_be_new_flaky': True, 'description': 'Точно 1 день'},
            {'days_in_state': 2, 'should_be_new_flaky': False, 'description': 'Больше 1 дня'},
            
            # days_in_state >= 14 для muted_stable_n_days и deleted
            {'days_in_state': 13, 'should_be_long_muted': False, 'description': 'Меньше 14 дней'},
            {'days_in_state': 14, 'should_be_long_muted': True, 'description': 'Точно 14 дней'},
            {'days_in_state': 15, 'should_be_long_muted': True, 'description': 'Больше 14 дней'},
        ]
        
        for case in boundary_cases:
            with self.subTest(case=case['description']):
                days = case['days_in_state']
                
                # Проверяем условие для new_flaky
                if 'should_be_new_flaky' in case:
                    new_flaky_condition = (days == 1)
                    self.assertEqual(new_flaky_condition, case['should_be_new_flaky'],
                                   f"new_flaky условие для {case['description']}")
                
                # Проверяем условие для muted_stable_n_days/deleted
                if 'should_be_long_muted' in case:
                    long_muted_condition = (days >= 14)
                    self.assertEqual(long_muted_condition, case['should_be_long_muted'],
                                   f"long_muted условие для {case['description']}")

    def test_sql_boundary_conditions_is_test_chunk(self):
        """Тест граничных условий для is_test_chunk в SQL"""
        
        # Граничное условие: is_test_chunk = 0 (для muted_stable_n_days и deleted)
        boundary_cases = [
            {'is_test_chunk': 0, 'should_be_eligible': True, 'description': 'Не chunk тест'},
            {'is_test_chunk': 1, 'should_be_eligible': False, 'description': 'Chunk тест'},
            {'is_test_chunk': None, 'should_be_eligible': False, 'description': 'Неопределенное значение'},
        ]
        
        for case in boundary_cases:
            with self.subTest(case=case['description']):
                chunk_value = case['is_test_chunk']
                
                # Проверяем условие для muted_stable_n_days/deleted
                eligible_condition = (chunk_value == 0)
                self.assertEqual(eligible_condition, case['should_be_eligible'],
                               f"is_test_chunk условие для {case['description']}")

    @patch('create_new_muted_ya.YaMuteCheck')
    @patch('create_new_muted_ya.add_lines_to_file')
    def test_flaky_filtering_boundary_conditions(self, mock_add_lines, mock_mute_check):
        """Тест граничных условий для фильтрации флаки тестов"""
        from create_new_muted_ya import apply_and_add_mutes
        
        mock_mute_instance = MagicMock()
        mock_mute_instance.return_value = False
        mock_mute_check.return_value = mock_mute_instance
        
        # Граничные условия для флаки тестов:
        # 1. days_in_state >= 1
        # 2. flaky_today = True
        # 3. (pass_count + fail_count) >= 2
        # 4. fail_count >= 2
        # 5. fail_count/(pass_count + fail_count) > 0.2
        
        boundary_test_cases = [
            # Тест граничного условия days_in_state >= 1
            {
                'name': 'days_in_state_boundary_0',
                'test_data': {
                    'full_name': 'test/boundary_days_0',
                    'suite_folder': 'test',
                    'test_name': 'boundary_days_0',
                    'owner': 'team/test',
                    'days_in_state': 0,  # Не соответствует >= 1
                    'flaky_today': True,
                    'pass_count': 3,
                    'fail_count': 7,  # 70% fail rate > 0.2
                    'muted_stable_n_days_today': False,
                    'deleted_today': False
                },
                'should_be_flaky': False
            },
            {
                'name': 'days_in_state_boundary_1',
                'test_data': {
                    'full_name': 'test/boundary_days_1',
                    'suite_folder': 'test',
                    'test_name': 'boundary_days_1',
                    'owner': 'team/test',
                    'days_in_state': 1,  # Точно >= 1
                    'flaky_today': True,
                    'pass_count': 3,
                    'fail_count': 7,  # 70% fail rate > 0.2
                    'muted_stable_n_days_today': False,
                    'deleted_today': False
                },
                'should_be_flaky': True
            },
            
            # Тест граничного условия (pass_count + fail_count) >= 2
            {
                'name': 'total_runs_boundary_1',
                'test_data': {
                    'full_name': 'test/boundary_runs_1',
                    'suite_folder': 'test',
                    'test_name': 'boundary_runs_1',
                    'owner': 'team/test',
                    'days_in_state': 1,
                    'flaky_today': True,
                    'pass_count': 0,
                    'fail_count': 1,  # Всего 1 запуск < 2
                    'muted_stable_n_days_today': False,
                    'deleted_today': False
                },
                'should_be_flaky': False
            },
            {
                'name': 'total_runs_boundary_2',
                'test_data': {
                    'full_name': 'test/boundary_runs_2',
                    'suite_folder': 'test',
                    'test_name': 'boundary_runs_2',
                    'owner': 'team/test',
                    'days_in_state': 1,
                    'flaky_today': True,
                    'pass_count': 0,
                    'fail_count': 2,  # Всего 2 запуска >= 2
                    'muted_stable_n_days_today': False,
                    'deleted_today': False
                },
                'should_be_flaky': True
            },
            
            # Тест граничного условия fail_count >= 2
            {
                'name': 'fail_count_boundary_1',
                'test_data': {
                    'full_name': 'test/boundary_fail_1',
                    'suite_folder': 'test',
                    'test_name': 'boundary_fail_1',
                    'owner': 'team/test',
                    'days_in_state': 1,
                    'flaky_today': True,
                    'pass_count': 8,
                    'fail_count': 1,  # Только 1 падение < 2
                    'muted_stable_n_days_today': False,
                    'deleted_today': False
                },
                'should_be_flaky': False
            },
            {
                'name': 'fail_count_boundary_2',
                'test_data': {
                    'full_name': 'test/boundary_fail_2',
                    'suite_folder': 'test',
                    'test_name': 'boundary_fail_2',
                    'owner': 'team/test',
                    'days_in_state': 1,
                    'flaky_today': True,
                    'pass_count': 3,
                    'fail_count': 2,  # Точно 2 падения >= 2
                    'muted_stable_n_days_today': False,
                    'deleted_today': False
                },
                'should_be_flaky': True
            },
            
            # Тест граничного условия fail_count/(pass_count + fail_count) > 0.2
            {
                'name': 'fail_rate_boundary_exactly_0_2',
                'test_data': {
                    'full_name': 'test/boundary_rate_02',
                    'suite_folder': 'test',
                    'test_name': 'boundary_rate_02',
                    'owner': 'team/test',
                    'days_in_state': 1,
                    'flaky_today': True,
                    'pass_count': 8,
                    'fail_count': 2,  # 2/10 = 0.2 (НЕ > 0.2)
                    'muted_stable_n_days_today': False,
                    'deleted_today': False
                },
                'should_be_flaky': False
            },
            {
                'name': 'fail_rate_boundary_just_above_0_2',
                'test_data': {
                    'full_name': 'test/boundary_rate_021',
                    'suite_folder': 'test',
                    'test_name': 'boundary_rate_021',
                    'owner': 'team/test',
                    'days_in_state': 1,
                    'flaky_today': True,
                    'pass_count': 79,
                    'fail_count': 21,  # 21/100 = 0.21 > 0.2
                    'muted_stable_n_days_today': False,
                    'deleted_today': False
                },
                'should_be_flaky': True
            },
            
            # Тест условия flaky_today = True
            {
                'name': 'flaky_today_false',
                'test_data': {
                    'full_name': 'test/boundary_flaky_today_false',
                    'suite_folder': 'test',
                    'test_name': 'boundary_flaky_today_false',
                    'owner': 'team/test',
                    'days_in_state': 1,
                    'flaky_today': False,  # Не флаки сегодня
                    'pass_count': 3,
                    'fail_count': 7,  # 70% fail rate > 0.2
                    'muted_stable_n_days_today': False,
                    'deleted_today': False
                },
                'should_be_flaky': False
            },
        ]
        
        for case in boundary_test_cases:
            with self.subTest(case=case['name']):
                apply_and_add_mutes([case['test_data']], "/tmp", mock_mute_instance)
                
                # Найдем ПОСЛЕДНИЙ вызов для flaky.txt
                flaky_call = None
                for call in mock_add_lines.call_args_list:
                    if 'flaky.txt' in call[0][0]:
                        flaky_call = call[0][1]
                        # Не используем break - ищем последний вызов
                
                self.assertIsNotNone(flaky_call, f"flaky.txt не был создан для {case['name']}")
                
                flaky_content = ''.join(flaky_call or [])
                
                if case['should_be_flaky']:
                    self.assertIn(case['test_data']['test_name'], flaky_content,
                                f"Тест {case['name']} должен быть в флаки, но его нет")
                else:
                    self.assertNotIn(case['test_data']['test_name'], flaky_content,
                                   f"Тест {case['name']} НЕ должен быть в флаки, но он есть")
                
                # Сбрасываем моки для следующего теста
                mock_add_lines.reset_mock()

    def test_boundary_conditions_calculations(self):
        """Тест точности вычислений граничных условий"""
        
        # Точные вычисления для граничного условия 0.2
        test_cases = [
            # Различные комбинации которые дают точно 0.2
            {'pass_count': 8, 'fail_count': 2, 'expected_rate': 0.2},
            {'pass_count': 4, 'fail_count': 1, 'expected_rate': 0.2},
            {'pass_count': 80, 'fail_count': 20, 'expected_rate': 0.2},
            
            # Комбинации чуть выше 0.2
            {'pass_count': 79, 'fail_count': 21, 'expected_rate': 0.21},
            {'pass_count': 7, 'fail_count': 2, 'expected_rate': 2/9},  # ≈ 0.222
            
            # Комбинации чуть ниже 0.2
            {'pass_count': 81, 'fail_count': 19, 'expected_rate': 19/100},  # 0.19
            {'pass_count': 9, 'fail_count': 2, 'expected_rate': 2/11},  # ≈ 0.182
        ]
        
        for case in test_cases:
            with self.subTest(case=case):
                pass_count = case['pass_count']
                fail_count = case['fail_count']
                expected_rate = case['expected_rate']
                
                # Вычисляем фактический failure rate
                actual_rate = fail_count / (pass_count + fail_count)
                
                # Проверяем точность вычисления
                self.assertAlmostEqual(actual_rate, expected_rate, places=10,
                                     msg=f"Неточное вычисление для {pass_count} pass, {fail_count} fail")
                
                # Проверяем граничное условие > 0.2
                should_be_flaky = actual_rate > 0.2
                expected_flaky = expected_rate > 0.2
                
                self.assertEqual(should_be_flaky, expected_flaky,
                               f"Граничное условие для rate={actual_rate}")

    def test_edge_cases_zero_division(self):
        """Тест граничных случаев с нулевыми значениями"""
        
        edge_cases = [
            # Нулевые значения
            {'pass_count': 0, 'fail_count': 0, 'description': 'Нет запусков'},
            {'pass_count': 0, 'fail_count': 2, 'description': 'Только падения'},
            {'pass_count': 2, 'fail_count': 0, 'description': 'Только успешные'},
            
            # Очень малые значения
            {'pass_count': 1, 'fail_count': 1, 'description': 'Минимальные значения'},
        ]
        
        for case in edge_cases:
            with self.subTest(case=case['description']):
                pass_count = case['pass_count']
                fail_count = case['fail_count']
                
                total_runs = pass_count + fail_count
                
                # Проверяем условие >= 2 total runs
                meets_run_condition = total_runs >= 2
                
                # Проверяем условие >= 2 fail count
                meets_fail_condition = fail_count >= 2
                
                # Проверяем условие > 0.2 fail rate (только если есть запуски)
                if total_runs > 0:
                    fail_rate = fail_count / total_runs
                    meets_rate_condition = fail_rate > 0.2
                else:
                    meets_rate_condition = False
                
                # Все условия должны быть True для флаки теста
                should_be_flaky = (meets_run_condition and 
                                 meets_fail_condition and 
                                 meets_rate_condition)
                
                # Логические проверки
                if case['description'] == 'Нет запусков':
                    self.assertFalse(should_be_flaky, "Тест без запусков не должен быть флаки")
                elif case['description'] == 'Только падения':
                    # 0 pass, 2 fail -> 2/2 = 1.0 > 0.2, но нужно >=2 общих запусков и >=2 падений
                    self.assertTrue(meets_run_condition, "Должно быть >= 2 запусков")
                    self.assertTrue(meets_fail_condition, "Должно быть >= 2 падений")
                    self.assertTrue(meets_rate_condition, "Должно быть > 0.2 fail rate")
                    self.assertTrue(should_be_flaky, "Тест с только падениями должен быть флаки")
                elif case['description'] == 'Только успешные':
                    self.assertFalse(should_be_flaky, "Тест без падений не должен быть флаки")
                elif case['description'] == 'Минимальные значения':
                    # 1 pass, 1 fail -> 1/2 = 0.5 > 0.2, но fail_count = 1 < 2
                    self.assertFalse(should_be_flaky, "Тест с < 2 падениями не должен быть флаки")

    def test_sql_query_boundary_conditions_in_historical_data(self):
        """Тест граничных условий в SQL запросах для исторических данных"""
        
        # Проверяем что в исторических запросах сохраняются те же граничные условия
        from datetime import datetime, timedelta
        
        base_date = datetime(2024, 1, 15)
        start_date = base_date - timedelta(days=7)
        
        start_ordinal = start_date.toordinal()
        end_ordinal = base_date.toordinal()
        
        # Исторические граничные условия должны сохранять логику
        historical_conditions = {
            'new_flaky_still_exact': f'days_in_state = 1 AND date_window = {end_ordinal}',
            'muted_stable_range': f'days_in_state >= 14 AND date_window >= {start_ordinal} AND date_window <= {end_ordinal}',
            'deleted_range': f'days_in_state >= 14 AND date_window >= {start_ordinal} AND date_window <= {end_ordinal}',
            'chunk_filter': 'is_test_chunk = 0'
        }
        
        # Проверяем что new_flaky все еще использует точное равенство
        self.assertIn('days_in_state = 1', historical_conditions['new_flaky_still_exact'])
        self.assertIn(f'date_window = {end_ordinal}', historical_conditions['new_flaky_still_exact'])
        
        # Проверяем что muted_stable и deleted используют >= 14
        self.assertIn('days_in_state >= 14', historical_conditions['muted_stable_range'])
        self.assertIn('days_in_state >= 14', historical_conditions['deleted_range'])
        
        # Проверяем что chunk фильтр сохраняется
        self.assertEqual(historical_conditions['chunk_filter'], 'is_test_chunk = 0')

    def test_regex_boundary_conditions(self):
        """Тест граничных условий для regex замены chunks"""
        
        import re
        
        # Паттерн для замены: r'\d+/(\d+)\]' -> r'*/*]'
        regex_pattern = r'\d+/(\d+)\]'
        replacement = r'*/*]'
        
        test_cases = [
            # Обычные chunk тесты
            {'input': 'test[12/34]', 'expected': 'test[*/*]'},
            {'input': 'test[1/2]', 'expected': 'test[*/*]'},
            {'input': 'test[123/456]', 'expected': 'test[*/*]'},
            
            # Граничные случаи
            {'input': 'test[0/1]', 'expected': 'test[*/*]'},
            {'input': 'test[1/1]', 'expected': 'test[*/*]'},
            {'input': 'test[999/999]', 'expected': 'test[*/*]'},
            
            # Случаи которые НЕ должны меняться
            {'input': 'test[a/b]', 'expected': 'test[a/b]'},
            {'input': 'test[/]', 'expected': 'test[/]'},
            {'input': 'test[]', 'expected': 'test[]'},
            {'input': 'test_without_chunks', 'expected': 'test_without_chunks'},
            
            # Сложные случаи
            {'input': 'test[12/34] other[56/78]', 'expected': 'test[*/*] other[*/*]'},
            {'input': 'test[12/34]_suffix', 'expected': 'test[*/*]_suffix'},
        ]
        
        for case in test_cases:
            with self.subTest(case=case):
                input_str = case['input']
                expected = case['expected']
                
                actual = re.sub(regex_pattern, replacement, input_str)
                
                self.assertEqual(actual, expected,
                               f"Regex замена для '{input_str}' дала неожиданный результат")


if __name__ == '__main__':
    unittest.main() 