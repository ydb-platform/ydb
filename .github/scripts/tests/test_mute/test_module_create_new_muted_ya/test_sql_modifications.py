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


class TestSQLModifications(unittest.TestCase):
    """Тесты для демонстрации изменений SQL запроса для исторических данных"""

    def setUp(self):
        """Настройка тестов"""
        self.base_date = datetime(2024, 1, 15)
        
    def test_current_sql_query_structure(self):
        """Тест текущей структуры SQL запроса (только сегодняшние данные)"""
        
        def get_current_query_structure():
            """Возвращает структуру текущего SQL запроса"""
            return {
                'main_data_filter': 'WHERE date_window = CurrentUtcDate()',
                'new_flaky_filter': 'WHERE state = "Flaky" AND days_in_state = 1 AND date_window = CurrentUtcDate()',
                'flaky_filter': 'WHERE state = "Flaky" AND date_window = CurrentUtcDate()',
                'muted_stable_filter': 'WHERE state = "Muted Stable" AND date_window = CurrentUtcDate()',
                'muted_stable_n_days_filter': 'WHERE state = "Muted Stable" AND days_in_state >= 14 AND date_window = CurrentUtcDate()',
                'deleted_filter': 'WHERE state = "no_runs" AND days_in_state >= 14 AND date_window = CurrentUtcDate()',
                'final_filter': 'WHERE date_window = CurrentUtcDate() AND branch = "{branch}" AND build_type = "{build_type}"'
            }
        
        current_structure = get_current_query_structure()
        
        # Проверяем что все фильтры используют CurrentUtcDate()
        for filter_name, filter_condition in current_structure.items():
            if 'date_window' in filter_condition:
                self.assertIn('CurrentUtcDate()', filter_condition, 
                             f"Фильтр {filter_name} должен использовать CurrentUtcDate()")

    def test_historical_sql_query_structure(self):
        """Тест структуры SQL запроса для исторических данных"""
        
        def get_historical_query_structure(days_back=7):
            """Возвращает структуру SQL запроса для исторических данных"""
            end_date = self.base_date
            start_date = end_date - timedelta(days=days_back)
            
            start_ordinal = start_date.toordinal()
            end_ordinal = end_date.toordinal()
            
            date_range_condition = f"date_window >= {start_ordinal} AND date_window <= {end_ordinal}"
            
            return {
                'main_data_filter': f'WHERE {date_range_condition}',
                'new_flaky_filter': f'WHERE state = "Flaky" AND days_in_state = 1 AND date_window = {end_ordinal}',  # Новые флаки только сегодня
                'flaky_filter': f'WHERE state = "Flaky" AND {date_range_condition}',
                'muted_stable_filter': f'WHERE state = "Muted Stable" AND {date_range_condition}',
                'muted_stable_n_days_filter': f'WHERE state = "Muted Stable" AND days_in_state >= 14 AND {date_range_condition}',
                'deleted_filter': f'WHERE state = "no_runs" AND days_in_state >= 14 AND {date_range_condition}',
                'final_filter': f'WHERE branch = "{{branch}}" AND build_type = "{{build_type}}"',  # Убираем фильтр по дате из финального WHERE
                'order_by': 'ORDER BY date_window DESC, full_name'
            }
        
        historical_structure = get_historical_query_structure(7)
        
        # Проверяем что основные фильтры используют диапазон дат
        range_filters = ['main_data_filter', 'flaky_filter', 'muted_stable_filter', 
                        'muted_stable_n_days_filter', 'deleted_filter']
        
        for filter_name in range_filters:
            filter_condition = historical_structure[filter_name]
            self.assertIn('date_window >=', filter_condition,
                         f"Фильтр {filter_name} должен использовать диапазон дат")
            self.assertIn('date_window <=', filter_condition,
                         f"Фильтр {filter_name} должен использовать диапазон дат")
        
        # Проверяем что новые флаки все еще используют сегодняшнюю дату
        self.assertIn(f'date_window = {self.base_date.toordinal()}', 
                     historical_structure['new_flaky_filter'])
        
        # Проверяем что добавлена сортировка
        self.assertEqual(historical_structure['order_by'], 'ORDER BY date_window DESC, full_name')

    def test_sql_query_generation_function(self):
        """Тест функции генерации SQL запроса"""
        
        def generate_historical_sql_query(branch='main', build_type='relwithdebinfo', days_back=7):
            """Генерирует SQL запрос для исторических данных"""
            end_date = self.base_date
            start_date = end_date - timedelta(days=days_back)
            
            start_ordinal = start_date.toordinal()
            end_ordinal = end_date.toordinal()
            
            date_range_condition = f"date_window >= {start_ordinal} AND date_window <= {end_ordinal}"
            
            query = f'''
            SELECT * from (
                SELECT data.*,
                CASE WHEN new_flaky.full_name IS NOT NULL THEN True ELSE False END AS new_flaky_today,
                CASE WHEN flaky.full_name IS NOT NULL THEN True ELSE False END AS flaky_today,
                CASE WHEN muted_stable.full_name IS NOT NULL THEN True ELSE False END AS muted_stable_today,
                CASE WHEN muted_stable_n_days.full_name IS NOT NULL THEN True ELSE False END AS muted_stable_n_days_today,
                CASE WHEN deleted.full_name IS NOT NULL THEN True ELSE False END AS deleted_today

                FROM
                (SELECT test_name, suite_folder, full_name, date_window, build_type, branch, 
                        days_ago_window, history, history_class, pass_count, mute_count, fail_count, 
                        skip_count, success_rate, summary, owner, is_muted, is_test_chunk, state, 
                        previous_state, state_change_date, days_in_state, previous_state_filtered, 
                        state_change_date_filtered, days_in_state_filtered, state_filtered
                FROM `test_results/analytics/tests_monitor`
                WHERE {date_range_condition}) as data
                
                LEFT JOIN 
                (SELECT full_name, build_type, branch
                    FROM `test_results/analytics/tests_monitor`
                    WHERE state = 'Flaky'
                    AND days_in_state = 1
                    AND date_window = {end_ordinal}
                    )as new_flaky
                ON 
                    data.full_name = new_flaky.full_name
                    AND data.build_type = new_flaky.build_type
                    AND data.branch = new_flaky.branch
                    
                LEFT JOIN 
                (SELECT full_name, build_type, branch
                    FROM `test_results/analytics/tests_monitor`
                    WHERE state = 'Flaky'
                    AND {date_range_condition}
                    )as flaky
                ON 
                    data.full_name = flaky.full_name
                    AND data.build_type = flaky.build_type
                    AND data.branch = flaky.branch
                    
                LEFT JOIN 
                (SELECT full_name, build_type, branch
                    FROM `test_results/analytics/tests_monitor`
                    WHERE state = 'Muted Stable'
                    AND {date_range_condition}
                    )as muted_stable
                ON 
                    data.full_name = muted_stable.full_name
                    AND data.build_type = muted_stable.build_type
                    AND data.branch = muted_stable.branch
                    
                LEFT JOIN 
                (SELECT full_name, build_type, branch
                    FROM `test_results/analytics/tests_monitor`
                    WHERE state = 'Muted Stable'
                    AND days_in_state >= 14
                    AND {date_range_condition}
                    AND is_test_chunk = 0
                    )as muted_stable_n_days
                ON 
                    data.full_name = muted_stable_n_days.full_name
                    AND data.build_type = muted_stable_n_days.build_type
                    AND data.branch = muted_stable_n_days.branch
               
                LEFT JOIN 
                (SELECT full_name, build_type, branch
                    FROM `test_results/analytics/tests_monitor`
                    WHERE state = 'no_runs'
                    AND days_in_state >= 14
                    AND {date_range_condition}
                    AND is_test_chunk = 0
                    )as deleted
                ON 
                    data.full_name = deleted.full_name
                    AND data.build_type = deleted.build_type
                    AND data.branch = deleted.branch
                ) 
                WHERE branch = '{branch}' AND build_type = '{build_type}'
                ORDER BY date_window DESC, full_name
            '''
            
            return query.strip()
        
        # Тест генерации запроса
        query = generate_historical_sql_query('main', 'relwithdebinfo', 7)
        
        # Проверяем основные элементы
        self.assertIn('SELECT * from', query)
        self.assertIn('date_window >=', query)
        self.assertIn('date_window <=', query)
        self.assertIn('ORDER BY date_window DESC, full_name', query)
        self.assertIn("branch = 'main'", query)
        self.assertIn("build_type = 'relwithdebinfo'", query)
        
        # Проверяем что не используется CurrentUtcDate в основных фильтрах
        main_data_section = query.split('LEFT JOIN')[0]
        self.assertNotIn('CurrentUtcDate()', main_data_section)
        
        # Проверяем что new_flaky все еще использует конкретную дату
        new_flaky_section = query.split('as new_flaky')[0].split('LEFT JOIN')[1]
        self.assertIn(f'date_window = {self.base_date.toordinal()}', new_flaky_section)

    def test_data_processing_modifications(self):
        """Тест модификаций обработки данных для исторических данных"""
        
        def process_historical_data(raw_data, current_date_ordinal):
            """Обрабатывает исторические данные для выявления паттернов"""
            
            # Группируем данные по тестам
            test_groups = {}
            for test in raw_data:
                test_name = test['full_name']
                if test_name not in test_groups:
                    test_groups[test_name] = []
                test_groups[test_name].append(test)
            
            # Анализируем каждый тест
            processed_tests = []
            for test_name, test_history in test_groups.items():
                # Сортируем по дате (новые сначала)
                test_history.sort(key=lambda x: x.get('date_window', 0), reverse=True)
                
                # Берем последнюю запись для основной информации
                latest_test = test_history[0]
                
                # Агрегируем данные за все дни
                total_pass = sum(test.get('pass_count', 0) for test in test_history)
                total_fail = sum(test.get('fail_count', 0) for test in test_history)
                total_runs = total_pass + total_fail
                
                # Вычисляем метрики
                overall_success_rate = (total_pass / total_runs * 100) if total_runs > 0 else 0
                overall_failure_rate = (total_fail / total_runs) if total_runs > 0 else 0
                
                # Определяем тренд (сравниваем первую и последнюю половину)
                mid_point = len(test_history) // 2
                if len(test_history) >= 4:
                    recent_tests = test_history[:mid_point]
                    older_tests = test_history[mid_point:]
                    
                    recent_fail_rate = sum(t.get('fail_count', 0) for t in recent_tests) / max(1, sum(t.get('pass_count', 0) + t.get('fail_count', 0) for t in recent_tests))
                    older_fail_rate = sum(t.get('fail_count', 0) for t in older_tests) / max(1, sum(t.get('pass_count', 0) + t.get('fail_count', 0) for t in older_tests))
                    
                    trend = 'worsening' if recent_fail_rate > older_fail_rate else 'improving'
                else:
                    trend = 'stable'
                
                # Создаем обогащенную запись
                processed_test = {
                    'full_name': test_name,
                    'suite_folder': latest_test.get('suite_folder'),
                    'test_name': latest_test.get('test_name'),
                    'owner': latest_test.get('owner'),
                    'latest_state': latest_test.get('state'),
                    'latest_success_rate': latest_test.get('success_rate', 0),
                    'overall_success_rate': overall_success_rate,
                    'overall_failure_rate': overall_failure_rate,
                    'total_runs': total_runs,
                    'days_with_data': len(test_history),
                    'trend': trend,
                    'is_current_flaky': latest_test.get('date_window') == current_date_ordinal and latest_test.get('flaky_today', False),
                    'is_current_deleted': latest_test.get('date_window') == current_date_ordinal and latest_test.get('deleted_today', False),
                    'is_current_muted_stable': latest_test.get('date_window') == current_date_ordinal and latest_test.get('muted_stable_n_days_today', False)
                }
                
                processed_tests.append(processed_test)
            
            return processed_tests
        
        # Создаем тестовые данные (минимум 4 записи для анализа тренда)
        current_date = self.base_date.toordinal()
        test_data = [
            # Тест с ухудшающимся трендом:
            # Recent (новые 2 дня): 11 fails / 20 runs = 55% fail rate
            # Older (старые 2 дня): 3 fails / 20 runs = 15% fail rate
            # 55% > 15% → trend = 'worsening'
            {'full_name': 'test1', 'suite_folder': 'suite1', 'test_name': 'test1', 'owner': 'team1',
             'state': 'Flaky', 'success_rate': 40, 'pass_count': 4, 'fail_count': 6, 'flaky_today': True,
             'date_window': current_date, 'deleted_today': False, 'muted_stable_n_days_today': False},
            {'full_name': 'test1', 'suite_folder': 'suite1', 'test_name': 'test1', 'owner': 'team1',
             'state': 'Flaky', 'success_rate': 50, 'pass_count': 5, 'fail_count': 5, 'flaky_today': False,
             'date_window': current_date - 1, 'deleted_today': False, 'muted_stable_n_days_today': False},
            {'full_name': 'test1', 'suite_folder': 'suite1', 'test_name': 'test1', 'owner': 'team1',
             'state': 'Stable', 'success_rate': 80, 'pass_count': 8, 'fail_count': 2, 'flaky_today': False,
             'date_window': current_date - 2, 'deleted_today': False, 'muted_stable_n_days_today': False},
            {'full_name': 'test1', 'suite_folder': 'suite1', 'test_name': 'test1', 'owner': 'team1',
             'state': 'Stable', 'success_rate': 90, 'pass_count': 9, 'fail_count': 1, 'flaky_today': False,
             'date_window': current_date - 3, 'deleted_today': False, 'muted_stable_n_days_today': False},
        ]
        
        # Тест обработки
        processed = process_historical_data(test_data, current_date)
        
        self.assertEqual(len(processed), 1)
        
        test1 = processed[0]
        self.assertEqual(test1['full_name'], 'test1')
        self.assertEqual(test1['latest_state'], 'Flaky')
        self.assertEqual(test1['days_with_data'], 4)
        self.assertEqual(test1['trend'], 'worsening')
        self.assertTrue(test1['is_current_flaky'])
        self.assertFalse(test1['is_current_deleted'])
        
        # Проверяем агрегированные метрики
        self.assertEqual(test1['total_runs'], 40)  # 4+6+5+5+8+2+9+1 = 40
        expected_overall_success = (26 / 40) * 100  # 65%
        self.assertEqual(test1['overall_success_rate'], expected_overall_success)

    def test_migration_checklist(self):
        """Тест чек-листа для миграции на исторические данные"""
        
        migration_steps = [
            {
                'step': 'Изменить execute_query для поддержки диапазона дат',
                'description': 'Добавить параметр days_back и изменить фильтры date_window',
                'code_change': 'WHERE date_window >= start_ordinal AND date_window <= end_ordinal'
            },
            {
                'step': 'Обновить apply_and_add_mutes для работы с историческими данными',
                'description': 'Добавить агрегацию данных по тестам за несколько дней',
                'code_change': 'Группировка по full_name и суммирование pass_count, fail_count'
            },
            {
                'step': 'Модифицировать логику определения флаки тестов',
                'description': 'Учитывать тренды и историю, а не только сегодняшнее состояние',
                'code_change': 'Анализ failure_rate за весь период + тренд'
            },
            {
                'step': 'Обновить генерацию отчетов',
                'description': 'Добавить информацию о трендах и исторических данных',
                'code_change': 'Включить trend, days_with_data, overall_success_rate'
            },
            {
                'step': 'Добавить конфигурацию периода анализа',
                'description': 'Параметр для настройки количества дней для анализа',
                'code_change': 'Аргумент --days-back в CLI'
            }
        ]
        
        # Проверяем что все шаги документированы
        self.assertEqual(len(migration_steps), 5)
        
        for step in migration_steps:
            self.assertIn('step', step)
            self.assertIn('description', step)
            self.assertIn('code_change', step)
            
            # Проверяем что описание не пустое
            self.assertGreater(len(step['description']), 10)
            self.assertGreater(len(step['code_change']), 5)


if __name__ == '__main__':
    unittest.main() 