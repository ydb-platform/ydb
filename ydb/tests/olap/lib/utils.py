import yatest.common
import logging


def get_external_param(name: str, default: str = '') -> str:
    """Получает внешний параметр из yatest.common.get_param"""
    try:
        value = yatest.common.get_param(name, default)
        logging.info(f"[Utils] get_external_param({name}) = '{value}' (default='{default}')")
        return value
    except Exception as e:
        logging.warning(f"[Utils] Error getting external param {name}: {e}, using default '{default}'")
        return default


def external_param_is_true(param_name: str) -> bool:
    """Проверяет, что внешний параметр равен 'true' (case-insensitive)"""
    value = get_external_param(param_name, '')
    result = value.lower() == 'true'
    logging.info(f"[Utils] external_param_is_true({param_name}) = {result} (value='{value}')")
    return result
