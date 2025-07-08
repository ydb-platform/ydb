#!/usr/bin/env python3

import unittest
import tempfile
import os
import sys
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta

# Настройка моков
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from mock_setup import setup_mocks
setup_mocks()


class TestHistoricalData(unittest.TestCase):
    """Тесты для работы с историческими данными в create_new_muted_ya.py"""

    def setUp(self):
        """Настройка тестов"""
        # Создаем базовую дату для тестирования
        self.base_date = datetime(2024, 1, 15)
        
        # Создаем данные за несколько дней
        self.historical_data = self._create_historical_test_data()
        
    def _create_historical_test_data(self):
        """Создает тестовые данные за несколько дней"""
        data = []
        
        # Тест который становится флаки постепенно
        for day_offset in range(7):  # 7 дней истории
            date_window = (self.base_date - timedelta(days=day_offset)).toordinal()
            
            # Тест с ухудшающейся стабильностью
            fail_rate = min(0.1 + (day_offset * 0.1), 0.8)  # От 10% до 80% падений
            pass_count = max(20 - (day_offset * 2), 2)  # От 20 до 2 успешных
            fail_count = int(pass_count * fail_rate / (1 - fail_rate))
            
            data.append({
                'full_name': 'ydb/tests/functional/degrading_test',
                'suite_folder': 'ydb/tests/functional',
                'test_name': 'degrading_test',
                'owner': 'team/testing',
                'success_rate': 100 - (fail_rate * 100),
                'days_in_state': day_offset + 1,
                'state': 'Flaky' if day_offset < 3 else 'Stable',
                'flaky_today': day_offset == 0,  # Только сегодня флаки
                'muted_stable_n_days_today': False,
                'deleted_today': False,
                'pass_count': pass_count,
                'fail_count': fail_count,
                'date_window': date_window
            })
        
        # Тест который был стабилен, но сегодня упал
        for day_offset in range(7):
            date_window = (self.base_date - timedelta(days=day_offset)).toordinal()
            
            # Стабильный тест который только сегодня начал падать
            if day_offset == 0:  # Сегодня
                pass_count, fail_count = 5, 15  # 25% success rate
                state = 'Flaky'
                flaky_today = True
            else:  # Предыдущие дни
                pass_count, fail_count = 20, 1  # 95% success rate
                state = 'Stable'
                flaky_today = False
            
            data.append({
                'full_name': 'ydb/tests/functional/sudden_flaky_test',
                'suite_folder': 'ydb/tests/functional',
                'test_name': 'sudden_flaky_test',
                'owner': 'team/testing',
                'success_rate': (pass_count / (pass_count + fail_count)) * 100,
                'days_in_state': 1 if day_offset == 0 else 30,
                'state': state,
                'flaky_today': flaky_today,
                'muted_stable_n_days_today': False,
                'deleted_today': False,
                'pass_count': pass_count,
                'fail_count': fail_count,
                'date_window': date_window
            })
        
        # Тест который был удален недавно
        for day_offset in range(7):
            date_window = (self.base_date - timedelta(days=day_offset)).toordinal()
            
            if day_offset < 3:  # Последние 3 дня - no_runs
                state = 'no_runs'
                deleted_today = day_offset == 0
                pass_count, fail_count = 0, 0
            else:  # Ранее был стабильным
                state = 'Stable'
                deleted_today = False
                pass_count, fail_count = 15, 1
            
            data.append({
                'full_name': 'ydb/tests/functional/deleted_test',
                'suite_folder': 'ydb/tests/functional',
                'test_name': 'deleted_test',
                'owner': 'team/testing',
                'success_rate': 0 if pass_count == 0 else (pass_count / (pass_count + fail_count)) * 100,
                'days_in_state': day_offset + 1 if day_offset < 3 else 30,
                'state': state,
                'flaky_today': False,
                'muted_stable_n_days_today': False,
                'deleted_today': deleted_today,
                'pass_count': pass_count,
                'fail_count': fail_count,
                'date_window': date_window
            })
        
        return data

    def test_historical_data_structure(self):
        """Тест структуры исторических данных"""
        # Проверяем что данные создались корректно
        self.assertEqual(len(self.historical_data), 21)  # 3 теста * 7 дней
        
        # Проверяем что есть данные за разные дни
        dates = set(test['date_window'] for test in self.historical_data)
        self.assertEqual(len(dates), 7)
        
        # Проверяем что есть разные состояния
        states = set(test['state'] for test in self.historical_data)
        self.assertIn('Flaky', states)
        self.assertIn('Stable', states)
        self.assertIn('no_runs', states)

    @patch('create_new_muted_ya.YaMuteCheck')
    @patch('create_new_muted_ya.add_lines_to_file')
    def test_historical_flaky_detection(self, mock_add_lines, mock_mute_check):
        """Тест выявления флаки тестов на основе исторических данных"""
        from create_new_muted_ya import apply_and_add_mutes
        
        mock_mute_instance = MagicMock()
        mock_mute_instance.return_value = False
        mock_mute_check.return_value = mock_mute_instance
        
        # Тестируем только данные за сегодняшний день (как в текущей логике)
        today_data = [test for test in self.historical_data if test['date_window'] == self.base_date.toordinal()]
        
        apply_and_add_mutes(today_data, "/tmp", mock_mute_instance)
        
        # Найдем вызов для flaky.txt
        flaky_call = None
        for call in mock_add_lines.call_args_list:
            if 'flaky.txt' in call[0][0]:
                flaky_call = call[0][1]
                break
        
        self.assertIsNotNone(flaky_call)
        
        # Проверяем содержимое
        flaky_content = ''.join(flaky_call or [])
        
        # sudden_flaky_test должен попасть в флаки (25% success rate сегодня)
        self.assertIn('sudden_flaky_test', flaky_content)
        
        # deleted_test не должен попасть в флаки (состояние no_runs)
        self.assertNotIn('deleted_test', flaky_content)

    @patch('create_new_muted_ya.YaMuteCheck')
    @patch('create_new_muted_ya.add_lines_to_file')
    def test_historical_trend_analysis(self, mock_add_lines, mock_mute_check):
        """Тест анализа трендов на основе исторических данных"""
        from create_new_muted_ya import apply_and_add_mutes
        
        mock_mute_instance = MagicMock()
        mock_mute_instance.return_value = False
        mock_mute_check.return_value = mock_mute_instance
        
        # Создаем данные с трендом ухудшения
        trend_data = []
        for day_offset in range(5):
            date_window = (self.base_date - timedelta(days=day_offset)).toordinal()
            
            # Тест с ухудшающимся трендом - только сегодняшний день для теста
            if day_offset == 0:  # Сегодняшний день
                success_rate = 30.0  # 30% success rate
                pass_count = 3
                fail_count = 7  # 7 падений из 10 запусков = 70% падений > 20%
                
                trend_data.append({
                    'full_name': 'ydb/tests/functional/trending_down_test',
                    'suite_folder': 'ydb/tests/functional',
                    'test_name': 'trending_down_test',
                    'owner': 'team/testing',
                    'success_rate': success_rate,
                    'days_in_state': 3,  # >= 1
                    'state': 'Flaky',
                    'flaky_today': True,  # Обязательно True для попадания в флаки
                    'muted_stable_n_days_today': False,
                    'deleted_today': False,
                    'pass_count': pass_count,
                    'fail_count': fail_count,
                    'date_window': date_window
                })
        
        # Тестируем только сегодняшние данные
        today_data = [test for test in trend_data if test['date_window'] == self.base_date.toordinal()]
        
        apply_and_add_mutes(today_data, "/tmp", mock_mute_instance)
        
        # Найдем вызов для flaky.txt
        flaky_call = None
        for call in mock_add_lines.call_args_list:
            if 'flaky.txt' in call[0][0]:
                flaky_call = call[0][1]
                break
        
        self.assertIsNotNone(flaky_call)
        
        flaky_content = ''.join(flaky_call or [])
        
        # Тест с плохим трендом должен попасть в флаки
        self.assertIn('trending_down_test', flaky_content)

    def test_aggregate_historical_data(self):
        """Тест агрегации исторических данных"""
        # Группируем данные по тестам
        test_groups = {}
        for test in self.historical_data:
            key = test['full_name']
            if key not in test_groups:
                test_groups[key] = []
            test_groups[key].append(test)
        
        # Проверяем что каждый тест имеет данные за 7 дней
        for test_name, test_data in test_groups.items():
            self.assertEqual(len(test_data), 7, f"Тест {test_name} должен иметь данные за 7 дней")
            
            # Проверяем что данные отсортированы по дням
            dates = [test['date_window'] for test in test_data]
            self.assertEqual(dates, sorted(dates, reverse=True))

    def test_historical_data_filtering(self):
        """Тест фильтрации исторических данных"""
        
        # Фильтруем данные за последние 3 дня
        recent_cutoff = (self.base_date - timedelta(days=3)).toordinal()
        recent_data = [test for test in self.historical_data if test['date_window'] >= recent_cutoff]
        
        # Должно быть 3 теста * 4 дня = 12 записей
        self.assertEqual(len(recent_data), 12)
        
        # Фильтруем только флаки тесты
        flaky_data = [test for test in self.historical_data if test['flaky_today']]
        
        # Должно быть только 2 теста помеченных как флаки сегодня
        self.assertEqual(len(flaky_data), 2)

    @patch('create_new_muted_ya.YaMuteCheck')
    @patch('create_new_muted_ya.add_lines_to_file')
    def test_historical_muted_stable_tracking(self, mock_add_lines, mock_mute_check):
        """Тест отслеживания стабильно замьюченных тестов в исторических данных"""
        from create_new_muted_ya import apply_and_add_mutes
        
        mock_mute_instance = MagicMock()
        mock_mute_instance.return_value = False
        mock_mute_check.return_value = mock_mute_instance
        
        # Создаем данные с мьютом который стабилен больше 14 дней
        stable_muted_data = []
        for day_offset in range(3):
            date_window = (self.base_date - timedelta(days=day_offset)).toordinal()
            
            stable_muted_data.append({
                'full_name': 'ydb/tests/functional/long_muted_test',
                'suite_folder': 'ydb/tests/functional',
                'test_name': 'long_muted_test',
                'owner': 'team/testing',
                'success_rate': 0,
                'days_in_state': 20,  # Больше 14 дней
                'state': 'Muted Stable',
                'flaky_today': False,
                'muted_stable_n_days_today': day_offset == 0,  # Только сегодня
                'deleted_today': False,
                'pass_count': 0,
                'fail_count': 10,
                'date_window': date_window
            })
        
        today_data = [test for test in stable_muted_data if test['date_window'] == self.base_date.toordinal()]
        
        apply_and_add_mutes(today_data, "/tmp", mock_mute_instance)
        
        # Найдем вызов для muted_stable.txt
        muted_stable_call = None
        for call in mock_add_lines.call_args_list:
            if 'muted_stable.txt' in call[0][0]:
                muted_stable_call = call[0][1]
                break
        
        self.assertIsNotNone(muted_stable_call)
        
        muted_stable_content = ''.join(muted_stable_call or [])
        
        # Тест должен попасть в muted stable
        self.assertIn('long_muted_test', muted_stable_content)

    def test_prepare_historical_query_parameters(self):
        """Тест подготовки параметров для исторического запроса"""
        
        # Подготавливаем параметры для запроса за последние N дней
        days_back = 7
        end_date = self.base_date
        start_date = end_date - timedelta(days=days_back)
        
        # Проверяем диапазон дат
        self.assertEqual((end_date - start_date).days, days_back)
        
        # Создаем условие для SQL запроса
        start_ordinal = start_date.toordinal()
        end_ordinal = end_date.toordinal()
        
        # Эмулируем условие SQL
        filtered_data = [
            test for test in self.historical_data 
            if start_ordinal <= test['date_window'] <= end_ordinal
        ]
        
        # Проверяем что получили данные за правильный период
        self.assertEqual(len(filtered_data), len(self.historical_data))  # Все данные в пределах 7 дней
        
        # Проверяем что есть данные за каждый день
        dates_in_range = set(test['date_window'] for test in filtered_data)
        self.assertEqual(len(dates_in_range), 7)


if __name__ == '__main__':
    unittest.main() 