import django
from django.core.cache.backends.base import InvalidCacheBackendError

try:
    if django.VERSION[:2] >= (1, 7):
        from django.core.cache import caches
        cache = caches['debug-panel']
    else:
        from django.core.cache import get_cache
        cache = get_cache('debug-panel')
except InvalidCacheBackendError:
    from django.core.cache import cache
