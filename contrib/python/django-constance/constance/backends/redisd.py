from pickle import loads, dumps
from threading import RLock
from time import monotonic

from django.core.exceptions import ImproperlyConfigured

from . import Backend
from .. import settings, utils, signals, config


class RedisBackend(Backend):

    def __init__(self):
        super().__init__()
        self._prefix = settings.REDIS_PREFIX
        connection_cls = settings.REDIS_CONNECTION_CLASS
        if connection_cls is not None:
            self._rd = utils.import_module_attr(connection_cls)()
        else:
            try:
                import redis
            except ImportError:
                raise ImproperlyConfigured(
                    "The Redis backend requires redis-py to be installed.")
            if isinstance(settings.REDIS_CONNECTION, str):
                self._rd = redis.from_url(settings.REDIS_CONNECTION)
            else:
                self._rd = redis.Redis(**settings.REDIS_CONNECTION)

    def add_prefix(self, key):
        return "%s%s" % (self._prefix, key)

    def get(self, key):
        value = self._rd.get(self.add_prefix(key))
        if value:
            return loads(value)
        return None

    def mget(self, keys):
        if not keys:
            return
        prefixed_keys = [self.add_prefix(key) for key in keys]
        for key, value in zip(keys, self._rd.mget(prefixed_keys)):
            if value:
                yield key, loads(value)

    def set(self, key, value):
        old_value = self.get(key)
        self._rd.set(self.add_prefix(key), dumps(value, protocol=settings.REDIS_PICKLE_VERSION))
        signals.config_updated.send(
            sender=config, key=key, old_value=old_value, new_value=value
        )


class CachingRedisBackend(RedisBackend):
    _sentinel = object()
    _lock = RLock()

    def __init__(self):
        super().__init__()
        self._timeout = settings.REDIS_CACHE_TIMEOUT
        self._cache = {}
        self._sentinel = object()

    def _has_expired(self, value):
        return value[0] <= monotonic()

    def _cache_value(self, key, new_value):
        self._cache[key] = (monotonic() + self._timeout, new_value)

    def get(self, key):
        value = self._cache.get(key, self._sentinel)

        if value is self._sentinel or self._has_expired(value):
            with self._lock:
                new_value = super().get(key)
                self._cache_value(key, new_value)
                return new_value

        return value[1]

    def set(self, key, value):
        with self._lock:
            super().set(key, value)
            self._cache_value(key, value)

    def mget(self, keys):
        if not keys:
            return
        for key in keys:
            value = self.get(key)
            if value is not None:
                yield key, value
