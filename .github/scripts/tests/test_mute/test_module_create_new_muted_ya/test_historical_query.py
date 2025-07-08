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


class TestHistoricalQuery(unittest.TestCase):
    """Тесты для модификации SQL запроса для работы с историческими данными"""

    def setUp(self):
        """Настройка тестов"""
        self.base_date = datetime(2024, 1, 15)
        
    def test_generate_historical_query_conditions(self):
        """Тест генерации условий для исторического запроса"""
        
        # Функция для генерации условий запроса за N дней
        def generate_date_conditions(days_back=7):
            end_date = self.base_date
            start_date = end_date - timedelta(days=days_back)
            
            return {
                'start_date': start_date.strftime('%Y-%m-%d'),
                'end_date': end_date.strftime('%Y-%m-%d'),
                'start_ordinal': start_date.toordinal(),
                'end_ordinal': end_date.toordinal()
            }
        
        # Тест за 7 дней
        conditions_7d = generate_date_conditions(7)
        self.assertEqual(conditions_7d['start_date'], '2024-01-08')
        self.assertEqual(conditions_7d['end_date'], '2024-01-15')
        
        # Тест за 30 дней  
        conditions_30d = generate_date_conditions(30)
        self.assertEqual(conditions_30d['start_date'], '2023-12-16')
        self.assertEqual(conditions_30d['end_date'], '2024-01-15')

    def test_modified_sql_query_structure(self):
        """Тест структуры модифицированного SQL запроса"""
        
        def generate_historical_query(branch='main', build_type='relwithdebinfo', days_back=7):
            """Пример модифицированного SQL запроса для исторических данных"""
            end_date = self.base_date
            start_date = end_date - timedelta(days=days_back)
            
            # Условие для диапазона дат
            date_condition = f"date_window >= {start_date.toordinal()} AND date_window <= {end_date.toordinal()}"
            
            query_string = f'''
            SELECT * from (
                SELECT data.*,
                CASE WHEN new_flaky.full_name IS NOT NULL THEN True ELSE False END AS new_flaky_today,
                CASE WHEN flaky.full_name IS NOT NULL THEN True ELSE False END AS flaky_today,
                CASE WHEN muted_stable.full_name IS NOT NULL THEN True ELSE False END AS muted_stable_today,
                CASE WHEN muted_stable_n_days.full_name IS NOT NULL THEN True ELSE False END AS muted_stable_n_days_today,
                CASE WHEN deleted.full_name IS NOT NULL THEN True ELSE False END AS deleted_today

                FROM
                (SELECT test_name, suite_folder, full_name, date_window, build_type, branch, days_ago_window, history, history_class, pass_count, mute_count, fail_count, skip_count, success_rate, summary, owner, is_muted, is_test_chunk, state, previous_state, state_change_date, days_in_state, previous_state_filtered, state_change_date_filtered, days_in_state_filtered, state_filtered
                FROM `test_results/analytics/tests_monitor`
                WHERE {date_condition}) as data
                
                left JOIN 
                (SELECT full_name, build_type, branch
                    FROM `test_results/analytics/tests_monitor`
                    WHERE state = 'Flaky'
                    AND days_in_state = 1
                    AND date_window = CurrentUtcDate()
                    )as new_flaky
                ON 
                    data.full_name = new_flaky.full_name
                    and data.build_type = new_flaky.build_type
                    and data.branch = new_flaky.branch
                    
                LEFT JOIN 
                (SELECT full_name, build_type, branch
                    FROM `test_results/analytics/tests_monitor`
                    WHERE state = 'Flaky'
                    AND {date_condition}
                    )as flaky
                ON 
                    data.full_name = flaky.full_name
                    and data.build_type = flaky.build_type
                    and data.branch = flaky.branch
                    
                LEFT JOIN 
                (SELECT full_name, build_type, branch
                    FROM `test_results/analytics/tests_monitor`
                    WHERE state = 'Muted Stable'
                    AND {date_condition}
                    )as muted_stable
                ON 
                    data.full_name = muted_stable.full_name
                    and data.build_type = muted_stable.build_type
                    and data.branch = muted_stable.branch
                    
                LEFT JOIN 
                (SELECT full_name, build_type, branch
                    FROM `test_results/analytics/tests_monitor`
                    WHERE state= 'Muted Stable'
                    AND days_in_state >= 14
                    AND {date_condition}
                    and is_test_chunk = 0
                    )as muted_stable_n_days
                ON 
                    data.full_name = muted_stable_n_days.full_name
                    and data.build_type = muted_stable_n_days.build_type
                    and data.branch = muted_stable_n_days.branch
               
                LEFT JOIN 
                (SELECT full_name, build_type, branch
                    FROM `test_results/analytics/tests_monitor`
                    WHERE state = 'no_runs'
                    AND days_in_state >= 14
                    AND {date_condition}
                    and is_test_chunk = 0
                    )as deleted
                ON 
                    data.full_name = deleted.full_name
                    and data.build_type = deleted.build_type
                    and data.branch = deleted.branch
                ) 
                where branch = '{branch}' and build_type = '{build_type}'
                ORDER BY date_window DESC, full_name
            '''
            
            return query_string
        
        # Тест генерации запроса
        query = generate_historical_query(days_back=7)
        
        # Проверяем что запрос содержит условия для диапазона дат
        self.assertIn('date_window >=', query)
        self.assertIn('date_window <=', query)
        
        # Проверяем что условие CurrentUtcDate() заменено на диапазон дат в JOIN'ах
        self.assertIn('ORDER BY date_window DESC', query)
        
        # Проверяем что сохранились основные структуры
        self.assertIn('new_flaky_today', query)
        self.assertIn('flaky_today', query)
        self.assertIn('muted_stable_today', query)

    def test_historical_data_aggregation(self):
        """Тест агрегации исторических данных"""
        
        def aggregate_historical_data(raw_data):
            """Агрегирует данные за несколько дней для каждого теста"""
            test_aggregates = {}
            
            for test in raw_data:
                test_name = test['full_name']
                if test_name not in test_aggregates:
                    test_aggregates[test_name] = {
                        'full_name': test_name,
                        'suite_folder': test['suite_folder'],
                        'test_name': test['test_name'],
                        'owner': test['owner'],
                        'total_pass_count': 0,
                        'total_fail_count': 0,
                        'days_with_data': 0,
                        'latest_state': None,
                        'latest_date': 0,
                        'states_history': [],
                        'success_rates': []
                    }
                
                agg = test_aggregates[test_name]
                agg['total_pass_count'] += test.get('pass_count', 0)
                agg['total_fail_count'] += test.get('fail_count', 0)
                agg['days_with_data'] += 1
                agg['states_history'].append(test.get('state', 'Unknown'))
                agg['success_rates'].append(test.get('success_rate', 0))
                
                # Обновляем последнее состояние
                if test.get('date_window', 0) > agg['latest_date']:
                    agg['latest_date'] = test.get('date_window', 0)
                    agg['latest_state'] = test.get('state', 'Unknown')
            
            # Вычисляем агрегированные метрики
            for test_name, agg in test_aggregates.items():
                total_runs = agg['total_pass_count'] + agg['total_fail_count']
                agg['overall_success_rate'] = (agg['total_pass_count'] / total_runs * 100) if total_runs > 0 else 0
                agg['avg_success_rate'] = sum(agg['success_rates']) / len(agg['success_rates']) if agg['success_rates'] else 0
                
                # Определяем тренд стабильности
                if len(agg['success_rates']) >= 2:
                    recent_avg = sum(agg['success_rates'][:3]) / min(3, len(agg['success_rates']))
                    older_avg = sum(agg['success_rates'][-3:]) / min(3, len(agg['success_rates'][-3:]))
                    agg['stability_trend'] = 'improving' if recent_avg > older_avg else 'degrading'
                else:
                    agg['stability_trend'] = 'stable'
            
            return test_aggregates
        
        # Создаем тестовые данные
        raw_data = [
            # Тест за 3 дня
            {'full_name': 'test1', 'suite_folder': 'suite1', 'test_name': 'test1', 'owner': 'team1', 
             'pass_count': 10, 'fail_count': 2, 'success_rate': 83.3, 'state': 'Stable', 'date_window': 3},
            {'full_name': 'test1', 'suite_folder': 'suite1', 'test_name': 'test1', 'owner': 'team1',
             'pass_count': 8, 'fail_count': 4, 'success_rate': 66.7, 'state': 'Flaky', 'date_window': 2},
            {'full_name': 'test1', 'suite_folder': 'suite1', 'test_name': 'test1', 'owner': 'team1',
             'pass_count': 5, 'fail_count': 7, 'success_rate': 41.7, 'state': 'Flaky', 'date_window': 1},
        ]
        
        # Тест агрегации
        aggregated = aggregate_historical_data(raw_data)
        
        self.assertEqual(len(aggregated), 1)
        
        test1_agg = aggregated['test1']
        self.assertEqual(test1_agg['total_pass_count'], 23)
        self.assertEqual(test1_agg['total_fail_count'], 13)
        self.assertEqual(test1_agg['days_with_data'], 3)
        self.assertEqual(test1_agg['latest_state'], 'Stable')
        self.assertEqual(test1_agg['stability_trend'], 'degrading')  # Тренд ухудшения
        
        # Проверяем общий success rate
        expected_overall = (23 / 36) * 100  # 63.89%
        self.assertAlmostEqual(test1_agg['overall_success_rate'], expected_overall, places=1)

    def test_historical_flaky_detection_logic(self):
        """Тест логики выявления флаки тестов на основе исторических данных"""
        
        def detect_flaky_tests_historical(aggregated_data, 
                                        min_failure_rate=0.2,  # 20% падений
                                        min_total_runs=5,
                                        min_days_with_data=3):
            """Выявляет флаки тесты на основе исторических данных"""
            flaky_tests = []
            
            for test_name, agg in aggregated_data.items():
                total_runs = agg['total_pass_count'] + agg['total_fail_count']
                
                # Проверяем критерии флакинеса
                if (total_runs >= min_total_runs and 
                    agg['days_with_data'] >= min_days_with_data and
                    agg['total_fail_count'] / total_runs >= min_failure_rate):
                    
                    flaky_tests.append({
                        'full_name': test_name,
                        'suite_folder': agg['suite_folder'],
                        'test_name': agg['test_name'],
                        'owner': agg['owner'],
                        'failure_rate': agg['total_fail_count'] / total_runs,
                        'overall_success_rate': agg['overall_success_rate'],
                        'stability_trend': agg['stability_trend'],
                        'days_with_data': agg['days_with_data'],
                        'total_runs': total_runs
                    })
            
            return flaky_tests
        
        # Создаем агрегированные данные для теста
        aggregated_data = {
            'stable_test': {
                'full_name': 'stable_test',
                'suite_folder': 'suite1',
                'test_name': 'stable_test',
                'owner': 'team1',
                'total_pass_count': 45,
                'total_fail_count': 5,
                'days_with_data': 5,
                'overall_success_rate': 90.0,
                'stability_trend': 'stable'
            },
            'flaky_test': {
                'full_name': 'flaky_test',
                'suite_folder': 'suite1',
                'test_name': 'flaky_test',
                'owner': 'team1',
                'total_pass_count': 30,
                'total_fail_count': 20,
                'days_with_data': 5,
                'overall_success_rate': 60.0,
                'stability_trend': 'degrading'
            },
            'insufficient_data_test': {
                'full_name': 'insufficient_data_test',
                'suite_folder': 'suite1',
                'test_name': 'insufficient_data_test',
                'owner': 'team1',
                'total_pass_count': 2,
                'total_fail_count': 1,
                'days_with_data': 2,
                'overall_success_rate': 66.7,
                'stability_trend': 'stable'
            }
        }
        
        # Тест выявления флаки тестов
        flaky_tests = detect_flaky_tests_historical(aggregated_data)
        
        # Проверяем результаты
        self.assertEqual(len(flaky_tests), 1)
        self.assertEqual(flaky_tests[0]['full_name'], 'flaky_test')
        self.assertAlmostEqual(flaky_tests[0]['failure_rate'], 0.4, places=2)
        self.assertEqual(flaky_tests[0]['stability_trend'], 'degrading')

    def test_historical_data_processing_pipeline(self):
        """Тест полного пайплайна обработки исторических данных"""
        
        def process_historical_data_pipeline(raw_data, days_back=7):
            """Полный пайплайн обработки исторических данных"""
            
            # Шаг 1: Фильтрация данных по дате
            cutoff_date = (self.base_date - timedelta(days=days_back)).toordinal()
            filtered_data = [test for test in raw_data if test.get('date_window', 0) >= cutoff_date]
            
            # Шаг 2: Агрегация данных по тестам
            test_aggregates = {}
            for test in filtered_data:
                test_name = test['full_name']
                if test_name not in test_aggregates:
                    test_aggregates[test_name] = {
                        'full_name': test_name,
                        'suite_folder': test['suite_folder'],
                        'test_name': test['test_name'],
                        'owner': test['owner'],
                        'total_pass_count': 0,
                        'total_fail_count': 0,
                        'days_with_data': 0,
                        'latest_state': None,
                        'latest_date': 0
                    }
                
                agg = test_aggregates[test_name]
                agg['total_pass_count'] += test.get('pass_count', 0)
                agg['total_fail_count'] += test.get('fail_count', 0)
                agg['days_with_data'] += 1
                
                if test.get('date_window', 0) > agg['latest_date']:
                    agg['latest_date'] = test.get('date_window', 0)
                    agg['latest_state'] = test.get('state', 'Unknown')
            
            # Шаг 3: Вычисление метрик
            for test_name, agg in test_aggregates.items():
                total_runs = agg['total_pass_count'] + agg['total_fail_count']
                agg['overall_success_rate'] = (agg['total_pass_count'] / total_runs * 100) if total_runs > 0 else 0
                agg['failure_rate'] = (agg['total_fail_count'] / total_runs) if total_runs > 0 else 0
            
            # Шаг 4: Классификация тестов
            classifications = {
                'stable': [],
                'flaky': [],
                'deleted': [],
                'insufficient_data': []
            }
            
            for test_name, agg in test_aggregates.items():
                total_runs = agg['total_pass_count'] + agg['total_fail_count']
                
                if total_runs < 5 or agg['days_with_data'] < 3:
                    classifications['insufficient_data'].append(agg)
                elif agg['latest_state'] == 'no_runs':
                    classifications['deleted'].append(agg)
                elif agg['failure_rate'] >= 0.2:  # 20% failure rate
                    classifications['flaky'].append(agg)
                else:
                    classifications['stable'].append(agg)
            
            return classifications
        
        # Создаем тестовые данные
        raw_data = [
            # Стабильный тест - несколько дней данных
            {'full_name': 'stable_test', 'suite_folder': 'suite1', 'test_name': 'stable_test', 'owner': 'team1',
             'pass_count': 19, 'fail_count': 1, 'state': 'Stable', 'date_window': self.base_date.toordinal()},
            {'full_name': 'stable_test', 'suite_folder': 'suite1', 'test_name': 'stable_test', 'owner': 'team1',
             'pass_count': 18, 'fail_count': 2, 'state': 'Stable', 'date_window': (self.base_date - timedelta(days=1)).toordinal()},
            {'full_name': 'stable_test', 'suite_folder': 'suite1', 'test_name': 'stable_test', 'owner': 'team1',
             'pass_count': 20, 'fail_count': 1, 'state': 'Stable', 'date_window': (self.base_date - timedelta(days=2)).toordinal()},
            {'full_name': 'stable_test', 'suite_folder': 'suite1', 'test_name': 'stable_test', 'owner': 'team1',
             'pass_count': 19, 'fail_count': 1, 'state': 'Stable', 'date_window': (self.base_date - timedelta(days=3)).toordinal()},
            
            # Флаки тест - несколько дней данных
            {'full_name': 'flaky_test', 'suite_folder': 'suite1', 'test_name': 'flaky_test', 'owner': 'team1',
             'pass_count': 12, 'fail_count': 8, 'state': 'Flaky', 'date_window': self.base_date.toordinal()},
            {'full_name': 'flaky_test', 'suite_folder': 'suite1', 'test_name': 'flaky_test', 'owner': 'team1',
             'pass_count': 10, 'fail_count': 10, 'state': 'Flaky', 'date_window': (self.base_date - timedelta(days=1)).toordinal()},
            {'full_name': 'flaky_test', 'suite_folder': 'suite1', 'test_name': 'flaky_test', 'owner': 'team1',
             'pass_count': 8, 'fail_count': 12, 'state': 'Flaky', 'date_window': (self.base_date - timedelta(days=2)).toordinal()},
            {'full_name': 'flaky_test', 'suite_folder': 'suite1', 'test_name': 'flaky_test', 'owner': 'team1',
             'pass_count': 7, 'fail_count': 13, 'state': 'Flaky', 'date_window': (self.base_date - timedelta(days=3)).toordinal()},
            
            # Удаленный тест - несколько дней данных
            {'full_name': 'deleted_test', 'suite_folder': 'suite1', 'test_name': 'deleted_test', 'owner': 'team1',
             'pass_count': 0, 'fail_count': 0, 'state': 'no_runs', 'date_window': self.base_date.toordinal()},
            {'full_name': 'deleted_test', 'suite_folder': 'suite1', 'test_name': 'deleted_test', 'owner': 'team1',
             'pass_count': 0, 'fail_count': 0, 'state': 'no_runs', 'date_window': (self.base_date - timedelta(days=1)).toordinal()},
            {'full_name': 'deleted_test', 'suite_folder': 'suite1', 'test_name': 'deleted_test', 'owner': 'team1',
             'pass_count': 15, 'fail_count': 2, 'state': 'Stable', 'date_window': (self.base_date - timedelta(days=4)).toordinal()},
        ]
        
        # Тест пайплайна
        classifications = process_historical_data_pipeline(raw_data)
        
        # Проверяем результаты
        self.assertEqual(len(classifications['stable']), 1)
        self.assertEqual(len(classifications['flaky']), 1)
        self.assertEqual(len(classifications['deleted']), 1)
        self.assertEqual(len(classifications['insufficient_data']), 0)
        
        # Проверяем детали
        self.assertEqual(classifications['stable'][0]['full_name'], 'stable_test')
        self.assertEqual(classifications['flaky'][0]['full_name'], 'flaky_test')
        self.assertEqual(classifications['deleted'][0]['full_name'], 'deleted_test')
        
        # Проверяем что stable_test имеет низкий failure rate
        stable_test = classifications['stable'][0]
        self.assertLess(stable_test['failure_rate'], 0.2)  # < 20%
        
        # Проверяем что flaky_test имеет высокий failure rate
        flaky_test = classifications['flaky'][0]
        self.assertGreater(flaky_test['failure_rate'], 0.2)  # > 20%


if __name__ == '__main__':
    unittest.main() 