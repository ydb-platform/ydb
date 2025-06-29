import yatest.common
import logging
import os


def get_external_param(name: str, default: str = '') -> str:
    """Получает внешний параметр из переменной окружения или аргументов командной строки"""
    result = os.getenv(name, default)
    if name == 'send-results':
        logging.info(f"[Utils] get_external_param({name}) = '{result}' (default='{default}')")
    return result


def external_param_is_true(param_name: str) -> bool:
    """Проверяет, является ли внешний параметр истинным"""
    result = get_external_param(param_name, '').lower() in ('1', 'true', 'yes', 'on')
    if param_name == 'send-results':
        logging.info(f"[Utils] external_param_is_true({param_name}) = {result}")
    return result
