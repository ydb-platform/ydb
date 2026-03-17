# coding: utf-8

from __future__ import unicode_literals
import os


def is_true(env_name, default_value=None):
    default_value = default_value or ''
    return os.environ.get(env_name, default_value).lower() in ('1', 'true', 'yes', 'on')


TOOLS_LOG_CONTEXT_PROVIDERS = [
    'django_tools_log_context.provider.request',
    'django_tools_log_context.provider.auth',
    # 'django_tools_log_context.provider.b2b',
    'django_tools_log_context.provider.endpoint',
]
TOOLS_LOG_CONTEXT_REQUESTS_MODULES = [
    'requests.sessions',
    'yt.packages.requests.sessions',
]
# Список заголовков для логирования
# string: имя заголовка
# bool: True - выводить в полученном виде, False - менять на звездочки (заголовки авторизации)
#    [
#      ('REMOTE_ADDR', True),
#      ('HTTP_AUTHORIZATION', False),
#    ]
TOOLS_LOG_CONTEXT_ALLOWED_HEADERS = [
]
TOOLS_LOG_CONTEXT_ENABLE_TRACKING = is_true('YLOG_TRACKING', '1')
TOOLS_LOG_CONTEXT_ENABLE_DB_TRACKING = True
TOOLS_LOG_CONTEXT_ENABLE_REDIS_TRACKING = is_true('YLOG_REDIS_TRACKING')
TOOLS_LOG_CONTEXT_ENABLE_HTTP_TRACKING = True
TOOLS_LOG_CONTEXT_ENABLE_HTTP_400_WARNING = False
TOOLS_LOG_CONTEXT_ENABLE_STACKTRACES = False
TOOLS_LOG_CONTEXT_RESPONSE_MAX_SIZE = 0  # characters
TOOLS_LOG_CONTEXT_SQL_WARNING_THRESHOLD = 500  # msec
TOOLS_LOG_CONTEXT_ALWAYS_SUBSTITUTE_SQL_PARAMS = False
TOOLS_LOG_CONTEXT_QUERY_TO_ANALYSE = True
TOOLS_LOG_CONTEXT_QUERY_PARAMS_BLACKLIST = []
