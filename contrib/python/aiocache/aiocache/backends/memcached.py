import asyncio

import aiomcache

from aiocache.base import BaseCache
from aiocache.serializers import JsonSerializer


class MemcachedBackend(BaseCache):
    def __init__(self, endpoint="127.0.0.1", port=11211, pool_size=2, **kwargs):
        super().__init__(**kwargs)
        self.endpoint = endpoint
        self.port = port
        self.pool_size = int(pool_size)
        self.client = aiomcache.Client(
            self.endpoint, self.port, pool_size=self.pool_size
        )

    async def _get(self, key, encoding="utf-8", _conn=None):
        value = await self.client.get(key)
        if encoding is None or value is None:
            return value
        return value.decode(encoding)

    async def _gets(self, key, encoding="utf-8", _conn=None):
        key = key.encode() if isinstance(key, str) else key
        _, token = await self.client.gets(key)
        return token

    async def _multi_get(self, keys, encoding="utf-8", _conn=None):
        values = []
        for value in await self.client.multi_get(*keys):
            if encoding is None or value is None:
                values.append(value)
            else:
                values.append(value.decode(encoding))
        return values

    async def _set(self, key, value, ttl=0, _cas_token=None, _conn=None):
        value = value.encode() if isinstance(value, str) else value
        if _cas_token is not None:
            return await self._cas(key, value, _cas_token, ttl=ttl, _conn=_conn)
        try:
            return await self.client.set(key, value, exptime=ttl or 0)
        except aiomcache.exceptions.ValidationException as e:
            raise TypeError("aiomcache error: {}".format(str(e)))

    async def _cas(self, key, value, token, ttl=None, _conn=None):
        return await self.client.cas(key, value, token, exptime=ttl or 0)

    async def _multi_set(self, pairs, ttl=0, _conn=None):
        tasks = []
        for key, value in pairs:
            value = str.encode(value) if isinstance(value, str) else value
            tasks.append(self.client.set(key, value, exptime=ttl or 0))

        try:
            await asyncio.gather(*tasks)
        except aiomcache.exceptions.ValidationException as e:
            raise TypeError("aiomcache error: {}".format(str(e)))

        return True

    async def _add(self, key, value, ttl=0, _conn=None):
        value = str.encode(value) if isinstance(value, str) else value
        try:
            ret = await self.client.add(key, value, exptime=ttl or 0)
        except aiomcache.exceptions.ValidationException as e:
            raise TypeError("aiomcache error: {}".format(str(e)))
        if not ret:
            raise ValueError("Key {} already exists, use .set to update the value".format(key))

        return True

    async def _exists(self, key, _conn=None):
        return await self.client.append(key, b"")

    async def _increment(self, key, delta, _conn=None):
        incremented = None
        try:
            if delta > 0:
                incremented = await self.client.incr(key, delta)
            else:
                incremented = await self.client.decr(key, abs(delta))
        except aiomcache.exceptions.ClientException as e:
            if "NOT_FOUND" in str(e):
                await self._set(key, str(delta).encode())
            else:
                raise TypeError("aiomcache error: {}".format(str(e)))

        return incremented or delta

    async def _expire(self, key, ttl, _conn=None):
        return await self.client.touch(key, ttl)

    async def _delete(self, key, _conn=None):
        return 1 if await self.client.delete(key) else 0

    async def _clear(self, namespace=None, _conn=None):
        if namespace:
            raise ValueError("MemcachedBackend doesnt support flushing by namespace")
        else:
            await self.client.flush_all()
        return True

    async def _raw(self, command, *args, encoding="utf-8", _conn=None, **kwargs):
        value = await getattr(self.client, command)(*args, **kwargs)
        if command in {"get", "multi_get"}:
            if encoding is not None and value is not None:
                return value.decode(encoding)
        return value

    async def _redlock_release(self, key, _):
        # Not ideal, should check the value coincides first but this would introduce
        # race conditions
        return await self._delete(key)

    async def _close(self, *args, _conn=None, **kwargs):
        await self.client.close()


class MemcachedCache(MemcachedBackend):
    """
    Memcached cache implementation with the following components as defaults:
        - serializer: :class:`aiocache.serializers.JsonSerializer`
        - plugins: []

    Config options are:

    :param serializer: obj derived from :class:`aiocache.serializers.BaseSerializer`.
    :param plugins: list of :class:`aiocache.plugins.BasePlugin` derived classes.
    :param namespace: string to use as default prefix for the key used in all operations of
        the backend. Default is None
    :param timeout: int or float in seconds specifying maximum timeout for the operations to last.
        By default its 5.
    :param endpoint: str with the endpoint to connect to. Default is 127.0.0.1.
    :param port: int with the port to connect to. Default is 11211.
    :param pool_size: int size for memcached connections pool. Default is 2.
    """

    NAME = "memcached"

    def __init__(self, serializer=None, **kwargs):
        super().__init__(serializer=serializer or JsonSerializer(), **kwargs)

    @classmethod
    def parse_uri_path(cls, path):
        return {}

    def _build_key(self, key, namespace=None):
        ns_key = super()._build_key(key, namespace=namespace).replace(" ", "_")
        return str.encode(ns_key)

    def __repr__(self):  # pragma: no cover
        return "MemcachedCache ({}:{})".format(self.endpoint, self.port)
