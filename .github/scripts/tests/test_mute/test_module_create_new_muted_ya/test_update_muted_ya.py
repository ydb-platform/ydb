#!/usr/bin/env python3
"""
Тесты интеграции для update_muted_ya функциональности
"""

import unittest
import os
import sys
from unittest.mock import patch, MagicMock

# Настройка моков
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from mock_setup import setup_mocks
setup_mocks()


class TestUpdateMutedYaIntegration(unittest.TestCase):
    """Тесты интеграции для update_muted_ya"""

    def setUp(self):
        """Настройка тестов"""
        self.sample_tests_data = [
            {
                'full_name': 'ydb/tests/functional/test1',
                'suite_folder': 'ydb/tests/functional',
                'test_name': 'test1',
                'owner': 'team/topics',
                'success_rate': 45.5,
                'days_in_state': 5,
                'state': 'Flaky',
                'flaky_today': True,
                'muted_stable_n_days_today': False,
                'deleted_today': False,
                'pass_count': 10,
                'fail_count': 15,
                'date_window': 19737,  # 2024-01-15
                'mute_count': 0,
                'skip_count': 0
            },
            {
                'full_name': 'ydb/core/tx/schemeshard/test2',
                'suite_folder': 'ydb/core/tx/schemeshard',
                'test_name': 'test2',
                'owner': 'team/storage', 
                'success_rate': 30.0,
                'days_in_state': 8,
                'state': 'Muted Stable',
                'flaky_today': False,
                'muted_stable_n_days_today': True,
                'deleted_today': False,
                'pass_count': 5,
                'fail_count': 20,
                'date_window': 19737,  # 2024-01-15
                'mute_count': 0,
                'skip_count': 0
            }
        ]

    @patch('create_new_muted_ya.YaMuteCheck')
    @patch('create_new_muted_ya.add_lines_to_file')
    @patch('create_new_muted_ya.logging.info')
    def test_apply_and_add_mutes_categories(self, mock_log, mock_add_lines, mock_mute_check):
        """Тест создания различных категорий файлов мьютинга"""
        from create_new_muted_ya import apply_and_add_mutes
        
        mock_mute_instance = MagicMock()
        mock_mute_instance.return_value = False
        mock_mute_check.return_value = mock_mute_instance
        
        output_path = "/tmp/test_output"
        
        result = apply_and_add_mutes(self.sample_tests_data, output_path, mock_mute_instance)
        
        # Проверяем что создаются основные файлы
        expected_files = [
            'deleted.txt',
            'muted_stable.txt',
            'flaky.txt',
            'new_muted_ya.txt',
            'new_muted_ya_with_flaky.txt',
            'muted_ya_sorted.txt',
        ]
        
        call_args_list = [call[0][0] for call in mock_add_lines.call_args_list]
        for expected_file in expected_files:
            expected_path = f"{output_path}/mute_update/{expected_file}"
            self.assertTrue(any(expected_path in call for call in call_args_list),
                          f"Файл {expected_file} не был создан")



if __name__ == '__main__':
    unittest.main() 