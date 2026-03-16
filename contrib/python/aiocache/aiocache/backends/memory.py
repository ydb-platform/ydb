import asyncio
from typing import Dict

from aiocache.base import BaseCache
from aiocache.serializers import NullSerializer


class SimpleMemoryBackend(BaseCache):
    """
    Wrapper around dict operations to use it as a cache backend
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self._cache: Dict[str, object] = {}
        self._handlers: Dict[str, asyncio.TimerHandle] = {}

    async def _get(self, key, encoding="utf-8", _conn=None):
        return self._cache.get(key)

    async def _gets(self, key, encoding="utf-8", _conn=None):
        return await self._get(key, encoding=encoding, _conn=_conn)

    async def _multi_get(self, keys, encoding="utf-8", _conn=None):
        return [self._cache.get(key) for key in keys]

    async def _set(self, key, value, ttl=None, _cas_token=None, _conn=None):
        if _cas_token is not None and _cas_token != self._cache.get(key):
            return 0

        if key in self._handlers:
            self._handlers[key].cancel()

        self._cache[key] = value
        if ttl:
            loop = asyncio.get_running_loop()
            self._handlers[key] = loop.call_later(ttl, self.__delete, key)
        return True

    async def _multi_set(self, pairs, ttl=None, _conn=None):
        for key, value in pairs:
            await self._set(key, value, ttl=ttl)
        return True

    async def _add(self, key, value, ttl=None, _conn=None):
        if key in self._cache:
            raise ValueError("Key {} already exists, use .set to update the value".format(key))

        await self._set(key, value, ttl=ttl)
        return True

    async def _exists(self, key, _conn=None):
        return key in self._cache

    async def _increment(self, key, delta, _conn=None):
        if key not in self._cache:
            self._cache[key] = delta
        else:
            try:
                self._cache[key] = int(self._cache[key]) + delta
            except ValueError:
                raise TypeError("Value is not an integer") from None
        return self._cache[key]

    async def _expire(self, key, ttl, _conn=None):
        if key in self._cache:
            handle = self._handlers.pop(key, None)
            if handle:
                handle.cancel()
            if ttl:
                loop = asyncio.get_running_loop()
                self._handlers[key] = loop.call_later(ttl, self.__delete, key)
            return True

        return False

    async def _delete(self, key, _conn=None):
        return self.__delete(key)

    async def _clear(self, namespace=None, _conn=None):
        if namespace:
            for key in list(self._cache):
                if key.startswith(namespace):
                    self.__delete(key)
        else:
            self._cache = {}
            self._handlers = {}
        return True

    async def _raw(self, command, *args, encoding="utf-8", _conn=None, **kwargs):
        return getattr(self._cache, command)(*args, **kwargs)

    async def _redlock_release(self, key, value):
        if self._cache.get(key) == value:
            return self.__delete(key)
        return 0

    def __delete(self, key):
        if self._cache.pop(key, None) is not None:
            handle = self._handlers.pop(key, None)
            if handle:
                handle.cancel()
            return 1

        return 0


class SimpleMemoryCache(SimpleMemoryBackend):
    """
    Memory cache implementation with the following components as defaults:
        - serializer: :class:`aiocache.serializers.NullSerializer`
        - plugins: None

    Config options are:

    :param serializer: obj derived from :class:`aiocache.serializers.BaseSerializer`.
    :param plugins: list of :class:`aiocache.plugins.BasePlugin` derived classes.
    :param namespace: string to use as default prefix for the key used in all operations of
        the backend. Default is None.
    :param timeout: int or float in seconds specifying maximum timeout for the operations to last.
        By default its 5.
    """

    NAME = "memory"

    def __init__(self, serializer=None, **kwargs):
        super().__init__(serializer=serializer or NullSerializer(), **kwargs)

    @classmethod
    def parse_uri_path(cls, path):
        return {}
