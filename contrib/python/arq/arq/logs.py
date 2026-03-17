from typing import Any


def default_log_config(verbose: bool) -> dict[str, Any]:
    """
    Setup default config. for dictConfig.

    :param verbose: level: DEBUG if True, INFO if False
    :return: dict suitable for ``logging.config.dictConfig``
    """
    log_level = 'DEBUG' if verbose else 'INFO'
    return {
        'version': 1,
        'disable_existing_loggers': False,
        'handlers': {
            'arq.standard': {'level': log_level, 'class': 'logging.StreamHandler', 'formatter': 'arq.standard'}
        },
        'formatters': {'arq.standard': {'format': '%(asctime)s: %(message)s', 'datefmt': '%H:%M:%S'}},
        'loggers': {'arq': {'handlers': ['arq.standard'], 'level': log_level}},
    }
