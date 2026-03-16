import pickle

from django.conf import settings

BACKEND = getattr(
    settings,
    'CONSTANCE_BACKEND',
    'constance.backends.redisd.RedisBackend'
)

CONFIG = getattr(settings, 'CONSTANCE_CONFIG', {})

CONFIG_FIELDSETS = getattr(settings, 'CONSTANCE_CONFIG_FIELDSETS', {})

ADDITIONAL_FIELDS = getattr(settings, 'CONSTANCE_ADDITIONAL_FIELDS', {})

FILE_ROOT = getattr(settings, 'CONSTANCE_FILE_ROOT', '')

DATABASE_CACHE_BACKEND = getattr(
    settings,
    'CONSTANCE_DATABASE_CACHE_BACKEND',
    None
)

DATABASE_CACHE_AUTOFILL_TIMEOUT = getattr(
    settings,
    'CONSTANCE_DATABASE_CACHE_AUTOFILL_TIMEOUT',
    60 * 60 * 24
)

DATABASE_PREFIX = getattr(settings, 'CONSTANCE_DATABASE_PREFIX', '')

REDIS_PREFIX = getattr(settings, 'CONSTANCE_REDIS_PREFIX', 'constance:')

REDIS_CACHE_TIMEOUT = getattr(settings, 'CONSTANCE_REDIS_CACHE_TIMEOUT', 60)

REDIS_CONNECTION_CLASS = getattr(
    settings,
    'CONSTANCE_REDIS_CONNECTION_CLASS',
    None
)

REDIS_CONNECTION = getattr(settings, 'CONSTANCE_REDIS_CONNECTION', {})

REDIS_PICKLE_VERSION = getattr(settings, 'CONSTANCE_REDIS_PICKLE_VERSION', pickle.DEFAULT_PROTOCOL)

SUPERUSER_ONLY = getattr(settings, 'CONSTANCE_SUPERUSER_ONLY', True)

IGNORE_ADMIN_VERSION_CHECK = getattr(
    settings,
    'CONSTANCE_IGNORE_ADMIN_VERSION_CHECK',
    False
)
