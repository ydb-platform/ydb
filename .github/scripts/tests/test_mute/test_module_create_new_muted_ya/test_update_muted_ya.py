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


class TestUpdateMutedYa(unittest.TestCase):
    """Тесты для функциональности update_muted_ya в create_new_muted_ya.py"""

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
                'flaky_today': True,
                'muted_stable_n_days_today': False,
                'deleted_today': False,
                'pass_count': 10,
                'fail_count': 15
            },
            {
                'full_name': 'ydb/core/tx/schemeshard/test2',
                'suite_folder': 'ydb/core/tx/schemeshard',
                'test_name': 'test2',
                'owner': 'team/storage', 
                'success_rate': 30.0,
                'days_in_state': 14,
                'state': 'Muted Stable',
                'flaky_today': False,
                'muted_stable_n_days_today': True,
                'deleted_today': False,
                'pass_count': 5,
                'fail_count': 20
            },
            {
                'full_name': 'ydb/library/actors/test3',
                'suite_folder': 'ydb/library/actors',
                'test_name': 'test3',
                'owner': 'team/core',
                'success_rate': 0.0,
                'days_in_state': 20,
                'state': 'no_runs',
                'flaky_today': False,
                'muted_stable_n_days_today': False,
                'deleted_today': True,
                'pass_count': 0,
                'fail_count': 0
            }
        ]

    @patch('create_new_muted_ya.YaMuteCheck')
    @patch('create_new_muted_ya.add_lines_to_file')
    @patch('create_new_muted_ya.logging.info')
    def test_apply_and_add_mutes_categories(self, mock_log, mock_add_lines, mock_mute_check):
        """Тест создания различных категорий файлов мьютинга"""
        from create_new_muted_ya import apply_and_add_mutes
        
        # Настройка мока YaMuteCheck
        mock_mute_instance = MagicMock()
        mock_mute_instance.return_value = False  # Нет уже замьюченных тестов
        mock_mute_check.return_value = mock_mute_instance
        
        output_path = "/tmp/test_output"
        
        result = apply_and_add_mutes(self.sample_tests_data, output_path, mock_mute_instance)
        
        # Проверяем что создаются все необходимые файлы
        expected_files = [
            'deleted.txt',
            'deleted_debug.txt', 
            'muted_stable.txt',
            'muted_stable_debug.txt',
            'flaky.txt',
            'flaky_debug.txt',
            'new_muted_ya.txt',
            'new_muted_ya_debug.txt',
            'new_muted_ya_with_flaky.txt',
            'new_muted_ya_with_flaky_debug.txt',
            'muted_ya_sorted.txt',
            'muted_ya_sorted_debug.txt',
            'unmuted_debug.txt',
            'deleted_tests_in_mute.txt',
            'deleted_tests_in_mute_debug.txt'
        ]
        
        # Проверяем что add_lines_to_file вызывается для каждого файла
        call_args_list = [call[0][0] for call in mock_add_lines.call_args_list]
        for expected_file in expected_files:
            expected_path = f"{output_path}/mute_update/{expected_file}"
            self.assertTrue(any(expected_path in call for call in call_args_list),
                          f"Файл {expected_file} не был создан")

    @patch('create_new_muted_ya.YaMuteCheck')
    @patch('create_new_muted_ya.add_lines_to_file')
    def test_flaky_tests_filtering(self, mock_add_lines, mock_mute_check):
        """Тест фильтрации флаки тестов"""
        from create_new_muted_ya import apply_and_add_mutes
        
        # Создаем тесты с разными характеристиками для проверки фильтрации
        test_data = [
            # Должен попасть в флаки (успех < 80%, достаточно падений)
            {
                'full_name': 'ydb/test/flaky1',
                'suite_folder': 'ydb/test',
                'test_name': 'flaky1',
                'owner': 'team/test',
                'success_rate': 60.0,
                'days_in_state': 3,
                'state': 'Flaky',
                'flaky_today': True,
                'muted_stable_n_days_today': False,
                'deleted_today': False,
                'pass_count': 6,  # 60% от 10
                'fail_count': 4   # 40% от 10, > 20%
            },
            # НЕ должен попасть в флаки (слишком высокий success rate)
            {
                'full_name': 'ydb/test/stable1',
                'suite_folder': 'ydb/test', 
                'test_name': 'stable1',
                'owner': 'team/test',
                'success_rate': 85.0,
                'days_in_state': 3,
                'state': 'Flaky',
                'flaky_today': True,
                'muted_stable_n_days_today': False,
                'deleted_today': False,
                'pass_count': 17,  # 85% от 20
                'fail_count': 3    # 15% от 20, < 20%
            },
            # НЕ должен попасть в флаки (мало запусков)
            {
                'full_name': 'ydb/test/rare1',
                'suite_folder': 'ydb/test',
                'test_name': 'rare1', 
                'owner': 'team/test',
                'success_rate': 50.0,
                'days_in_state': 3,
                'state': 'Flaky',
                'flaky_today': True,
                'muted_stable_n_days_today': False,
                'deleted_today': False,
                'pass_count': 1,  # Всего 1+1=2 запуска, < порога
                'fail_count': 1
            }
        ]
        
        mock_mute_instance = MagicMock()
        mock_mute_instance.return_value = False
        mock_mute_check.return_value = mock_mute_instance
        
        apply_and_add_mutes(test_data, "/tmp", mock_mute_instance)
        
        # Найдем ПОСЛЕДНИЙ вызов для flaky.txt
        flaky_call = None
        for call in mock_add_lines.call_args_list:
            if 'flaky.txt' in call[0][0]:
                flaky_call = call[0][1]  # Содержимое файла
                # Не используем break - ищем последний вызов
        
        self.assertIsNotNone(flaky_call)
        
        # Проверяем что только нужные тесты попали в флаки
        flaky_content = ''.join(flaky_call or [])
        self.assertIn('flaky1', flaky_content)
        self.assertNotIn('stable1', flaky_content)
        self.assertNotIn('rare1', flaky_content)

    @patch('create_new_muted_ya.YaMuteCheck')
    @patch('create_new_muted_ya.add_lines_to_file')
    def test_wildcard_replacement(self, mock_add_lines, mock_mute_check):
        """Тест замены цифр на wildcards в именах тестов"""
        from create_new_muted_ya import apply_and_add_mutes
        
        test_data = [
            {
                'full_name': 'ydb/test/chunk[12/34]',
                'suite_folder': 'ydb/test',
                'test_name': 'chunk[12/34]',
                'owner': 'team/test',
                'success_rate': 50.0,
                'days_in_state': 3,
                'state': 'Flaky',
                'flaky_today': True,
                'muted_stable_n_days_today': False,
                'deleted_today': False,
                'pass_count': 5,
                'fail_count': 5
            }
        ]
        
        mock_mute_instance = MagicMock()
        mock_mute_instance.return_value = False
        mock_mute_check.return_value = mock_mute_instance
        
        apply_and_add_mutes(test_data, "/tmp", mock_mute_instance)
        
        # Найдем ПОСЛЕДНИЙ вызов для flaky.txt
        flaky_call = None
        for call in mock_add_lines.call_args_list:
            if 'flaky.txt' in call[0][0]:
                flaky_call = call[0][1]
                # Не используем break - ищем последний вызов
        
        # Проверяем что числа заменены на wildcards
        flaky_content = ''.join(flaky_call or [])
        self.assertIn('chunk[*/*]', flaky_content)
        self.assertNotIn('chunk[12/34]', flaky_content)

    @patch('create_new_muted_ya.execute_query')
    @patch('create_new_muted_ya.apply_and_add_mutes')
    @patch('ydb.Driver')
    @patch('builtins.open', create=True)
    def test_mute_worker_update_muted_ya_mode(self, mock_open, mock_driver, mock_apply, mock_execute):
        """Тест режима update_muted_ya в mute_worker"""
        from create_new_muted_ya import mute_worker
        import argparse
        
        # Создаем аргументы для режима update_muted_ya
        args = argparse.Namespace()
        args.mode = 'update_muted_ya'
        args.output_folder = '/tmp/test_output'
        args.branch = 'main'
        
        # Мокируем файл muted_ya.txt
        mock_file_content = "ydb/tests/functional/tpc test1\nydb/core/tx/schemeshard test2\n"
        mock_open.return_value.__enter__.return_value.read.return_value = mock_file_content
        
        # Настройка моков
        mock_execute.return_value = self.sample_tests_data
        mock_driver_instance = MagicMock()
        mock_driver.return_value.__enter__.return_value = mock_driver_instance
        mock_apply.return_value = 5  # Возвращает количество замьюченных тестов
        
        # Мокируем os.makedirs
        with patch('create_new_muted_ya.os.makedirs') as mock_makedirs:
            result = mute_worker(args)
        
        # Проверяем что функции были вызваны правильно
        mock_execute.assert_called_once_with(mock_driver_instance, 'main')
        mock_makedirs.assert_called_once_with('/tmp/test_output', exist_ok=True)
        mock_apply.assert_called_once()
        
        # Проверяем что результат корректный
        self.assertIsNone(result)  # Функция не возвращает значение при успехе

    @patch('create_new_muted_ya.YaMuteCheck')
    @patch('create_new_muted_ya.add_lines_to_file')
    def test_unmuted_tests_tracking(self, mock_add_lines, mock_mute_check):
        """Тест отслеживания размьюченных тестов"""
        from create_new_muted_ya import apply_and_add_mutes
        
        # Настройка: некоторые тесты уже замьючены
        def mute_check_side_effect(suite, test):
            return suite == 'ydb/tests/functional/tpc/large' and test == 'test1'
        
        mock_mute_instance = MagicMock()
        mock_mute_instance.side_effect = mute_check_side_effect
        mock_mute_check.return_value = mock_mute_instance
        
        # Тест который замьючен, но теперь стабилен (должен быть размьючен)
        test_data = [
            {
                'full_name': 'ydb/tests/functional/tpc/large/test1',
                'suite_folder': 'ydb/tests/functional/tpc/large',
                'test_name': 'test1',
                'owner': 'team/topics',
                'success_rate': 95.0,
                'days_in_state': 15,
                'state': 'Muted Stable',
                'flaky_today': False,
                'muted_stable_n_days_today': True,
                'deleted_today': False,
                'pass_count': 95,
                'fail_count': 5
            }
        ]
        
        apply_and_add_mutes(test_data, "/tmp", mock_mute_instance)
        
        # Найдем ПОСЛЕДНИЙ вызов для unmuted_debug.txt
        unmuted_call = None
        for call in mock_add_lines.call_args_list:
            if 'unmuted_debug.txt' in call[0][0]:
                unmuted_call = call[0][1]
                # Не используем break - ищем последний вызов
        
        self.assertIsNotNone(unmuted_call)
        unmuted_content = ''.join(unmuted_call or [])
        self.assertIn('test1', unmuted_content)
        self.assertIn('Muted Stable', unmuted_content)

    def test_error_handling_in_apply_and_add_mutes(self):
        """Тест обработки ошибок в apply_and_add_mutes"""
        from create_new_muted_ya import apply_and_add_mutes
        
        # Данные с отсутствующими ключами для генерации KeyError
        invalid_data = [
            {
                'full_name': 'test',
                # Отсутствуют обязательные поля: flaky_today, fail_count
            }
        ]
        
        mock_mute_instance = MagicMock()
        
        with patch('create_new_muted_ya.logging.error') as mock_error:
            result = apply_and_add_mutes(invalid_data, "/tmp", mock_mute_instance)
            
            # Проверяем что ошибка была залогирована
            mock_error.assert_called()
            # Функция должна вернуть пустой список при ошибке
            self.assertEqual(result, [])


if __name__ == '__main__':
    unittest.main(verbosity=2) 