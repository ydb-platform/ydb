#!/usr/bin/env python3
"""
Общая настройка моков для всех тестов
"""

import sys
import os
from unittest.mock import MagicMock


def setup_mocks():
    """Настройка всех необходимых моков для тестирования"""
    
    # Мокируем ydb
    sys.modules['ydb'] = MagicMock()
    
    # Правильно мокируем requests и его подмодули
    requests_mock = MagicMock()
    requests_mock.adapters = MagicMock()
    requests_mock.get = MagicMock()
    requests_mock.post = MagicMock()
    requests_mock.put = MagicMock()
    requests_mock.delete = MagicMock()
    requests_mock.patch = MagicMock()
    requests_mock.Response = MagicMock()
    requests_mock.Session = MagicMock()
    requests_mock.exceptions = MagicMock()
    sys.modules['requests'] = requests_mock
    sys.modules['requests.adapters'] = requests_mock.adapters
    
    # Мокируем github
    sys.modules['github'] = MagicMock()
    sys.modules['github.Github'] = MagicMock()
    
    # Мокируем configparser
    mock_config = MagicMock()
    mock_config.return_value.__getitem__.return_value = {
        'DATABASE_ENDPOINT': 'fake_endpoint',
        'DATABASE_PATH': 'fake_path'
    }
    sys.modules['configparser'] = MagicMock()
    sys.modules['configparser'].ConfigParser = mock_config
    
    # Мокируем переменные окружения
    os.environ['GITHUB_TOKEN'] = 'fake_token'
    os.environ['CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS'] = 'fake_credentials'
    
    # Добавляем путь к модулям
    parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sys.path.insert(0, parent_dir) 