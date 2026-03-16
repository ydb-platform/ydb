import asyncio
import functools
import logging
import os
import time
from enum import Enum
from types import TracebackType
from typing import Callable, Optional, Set, Type

from aiocache import serializers


logger = logging.getLogger(__name__)

SENTINEL = object()


class API:

    CMDS: Set[Callable[..., object]] = set()

    @classmethod
    def register(cls, func):
        API.CMDS.add(func)
        return func

    @classmethod
    def unregister(cls, func):
        API.CMDS.discard(func)

    @classmethod
    def timeout(cls, func):
        """
        This decorator sets a maximum timeout for a coroutine to execute. The timeout can be both
        set in the ``self.timeout`` attribute or in the ``timeout`` kwarg of the function call.
        I.e if you have a function ``get(self, key)``, if its decorated with this decorator, you
        will be able to call it with ``await get(self, "my_key", timeout=4)``.

        Use 0 or None to disable the timeout.
        """
        NOT_SET = "NOT_SET"

        @functools.wraps(func)
        async def _timeout(self, *args, timeout=NOT_SET, **kwargs):
            timeout = self.timeout if timeout == NOT_SET else timeout
            if timeout == 0 or timeout is None:
                return await func(self, *args, **kwargs)
            return await asyncio.wait_for(func(self, *args, **kwargs), timeout)

        return _timeout

    @classmethod
    def aiocache_enabled(cls, fake_return=None):
        """
        Use this decorator to be able to fake the return of the function by setting the
        ``AIOCACHE_DISABLE`` environment variable
        """

        def enabled(func):
            @functools.wraps(func)
            async def _enabled(*args, **kwargs):
                if os.getenv("AIOCACHE_DISABLE") == "1":
                    return fake_return
                return await func(*args, **kwargs)

            return _enabled

        return enabled

    @classmethod
    def plugins(cls, func):
        @functools.wraps(func)
        async def _plugins(self, *args, **kwargs):
            start = time.monotonic()
            for plugin in self.plugins:
                await getattr(plugin, "pre_{}".format(func.__name__))(self, *args, **kwargs)

            ret = await func(self, *args, **kwargs)

            end = time.monotonic()
            for plugin in self.plugins:
                await getattr(plugin, "post_{}".format(func.__name__))(
                    self, *args, took=end - start, ret=ret, **kwargs
                )
            return ret

        return _plugins


class BaseCache:
    """
    Base class that agregates the common logic for the different caches that may exist. Cache
    related available options are:

    :param serializer: obj derived from :class:`aiocache.serializers.BaseSerializer`. Default is
        :class:`aiocache.serializers.StringSerializer`.
    :param plugins: list of :class:`aiocache.plugins.BasePlugin` derived classes. Default is empty
        list.
    :param namespace: string to use as default prefix for the key used in all operations of
        the backend. Default is None
    :param key_builder: alternative callable to build the key. Receives the key and the namespace
        as params and should return something that can be used as key by the underlying backend.
    :param timeout: int or float in seconds specifying maximum timeout for the operations to last.
        By default its 5. Use 0 or None if you want to disable it.
    :param ttl: int the expiration time in seconds to use as a default in all operations of
        the backend. It can be overriden in the specific calls.
    """

    NAME: str

    def __init__(
        self, serializer=None, plugins=None, namespace=None, key_builder=None, timeout=5, ttl=None
    ):
        self.timeout = float(timeout) if timeout is not None else timeout
        self.namespace = namespace
        self.ttl = float(ttl) if ttl is not None else ttl
        self.build_key = key_builder or self._build_key

        self._serializer = None
        self.serializer = serializer or serializers.StringSerializer()

        self._plugins = None
        self.plugins = plugins or []

    @property
    def serializer(self):
        return self._serializer

    @serializer.setter
    def serializer(self, value):
        self._serializer = value

    @property
    def plugins(self):
        return self._plugins

    @plugins.setter
    def plugins(self, value):
        self._plugins = value

    @API.register
    @API.aiocache_enabled(fake_return=True)
    @API.timeout
    @API.plugins
    async def add(self, key, value, ttl=SENTINEL, dumps_fn=None, namespace=None, _conn=None):
        """
        Stores the value in the given key with ttl if specified. Raises an error if the
        key already exists.

        :param key: str
        :param value: obj
        :param ttl: int the expiration time in seconds. Due to memcached
            restrictions if you want compatibility use int. In case you
            need miliseconds, redis and memory support float ttls
        :param dumps_fn: callable alternative to use as dumps function
        :param namespace: str alternative namespace to use
        :param timeout: int or float in seconds specifying maximum timeout
            for the operations to last
        :returns: True if key is inserted
        :raises:
            - ValueError if key already exists
            - :class:`asyncio.TimeoutError` if it lasts more than self.timeout
        """
        start = time.monotonic()
        dumps = dumps_fn or self._serializer.dumps
        ns = namespace if namespace is not None else self.namespace
        ns_key = self.build_key(key, namespace=ns)

        await self._add(ns_key, dumps(value), ttl=self._get_ttl(ttl), _conn=_conn)

        logger.debug("ADD %s %s (%.4f)s", ns_key, True, time.monotonic() - start)
        return True

    async def _add(self, key, value, ttl, _conn=None):
        raise NotImplementedError()

    @API.register
    @API.aiocache_enabled()
    @API.timeout
    @API.plugins
    async def get(self, key, default=None, loads_fn=None, namespace=None, _conn=None):
        """
        Get a value from the cache. Returns default if not found.

        :param key: str
        :param default: obj to return when key is not found
        :param loads_fn: callable alternative to use as loads function
        :param namespace: str alternative namespace to use
        :param timeout: int or float in seconds specifying maximum timeout
            for the operations to last
        :returns: obj loaded
        :raises: :class:`asyncio.TimeoutError` if it lasts more than self.timeout
        """
        start = time.monotonic()
        loads = loads_fn or self._serializer.loads
        ns = namespace if namespace is not None else self.namespace
        ns_key = self.build_key(key, namespace=ns)

        value = loads(await self._get(ns_key, encoding=self.serializer.encoding, _conn=_conn))

        logger.debug("GET %s %s (%.4f)s", ns_key, value is not None, time.monotonic() - start)
        return value if value is not None else default

    async def _get(self, key, encoding, _conn=None):
        raise NotImplementedError()

    async def _gets(self, key, encoding="utf-8", _conn=None):
        raise NotImplementedError()

    @API.register
    @API.aiocache_enabled(fake_return=[])
    @API.timeout
    @API.plugins
    async def multi_get(self, keys, loads_fn=None, namespace=None, _conn=None):
        """
        Get multiple values from the cache, values not found are Nones.

        :param keys: list of str
        :param loads_fn: callable alternative to use as loads function
        :param namespace: str alternative namespace to use
        :param timeout: int or float in seconds specifying maximum timeout
            for the operations to last
        :returns: list of objs
        :raises: :class:`asyncio.TimeoutError` if it lasts more than self.timeout
        """
        start = time.monotonic()
        loads = loads_fn or self._serializer.loads
        ns = namespace if namespace is not None else self.namespace

        ns_keys = [self.build_key(key, namespace=ns) for key in keys]
        values = [
            loads(value)
            for value in await self._multi_get(
                ns_keys, encoding=self.serializer.encoding, _conn=_conn
            )
        ]

        logger.debug(
            "MULTI_GET %s %d (%.4f)s",
            ns_keys,
            len([value for value in values if value is not None]),
            time.monotonic() - start,
        )
        return values

    async def _multi_get(self, keys, encoding, _conn=None):
        raise NotImplementedError()

    @API.register
    @API.aiocache_enabled(fake_return=True)
    @API.timeout
    @API.plugins
    async def set(
        self, key, value, ttl=SENTINEL, dumps_fn=None, namespace=None, _cas_token=None, _conn=None
    ):
        """
        Stores the value in the given key with ttl if specified

        :param key: str
        :param value: obj
        :param ttl: int the expiration time in seconds. Due to memcached
            restrictions if you want compatibility use int. In case you
            need miliseconds, redis and memory support float ttls
        :param dumps_fn: callable alternative to use as dumps function
        :param namespace: str alternative namespace to use
        :param timeout: int or float in seconds specifying maximum timeout
            for the operations to last
        :returns: True if the value was set
        :raises: :class:`asyncio.TimeoutError` if it lasts more than self.timeout
        """
        start = time.monotonic()
        dumps = dumps_fn or self._serializer.dumps
        ns = namespace if namespace is not None else self.namespace
        ns_key = self.build_key(key, namespace=ns)

        res = await self._set(
            ns_key, dumps(value), ttl=self._get_ttl(ttl), _cas_token=_cas_token, _conn=_conn
        )

        logger.debug("SET %s %d (%.4f)s", ns_key, True, time.monotonic() - start)
        return res

    async def _set(self, key, value, ttl, _cas_token=None, _conn=None):
        raise NotImplementedError()

    @API.register
    @API.aiocache_enabled(fake_return=True)
    @API.timeout
    @API.plugins
    async def multi_set(self, pairs, ttl=SENTINEL, dumps_fn=None, namespace=None, _conn=None):
        """
        Stores multiple values in the given keys.

        :param pairs: list of two element iterables. First is key and second is value
        :param ttl: int the expiration time in seconds. Due to memcached
            restrictions if you want compatibility use int. In case you
            need miliseconds, redis and memory support float ttls
        :param dumps_fn: callable alternative to use as dumps function
        :param namespace: str alternative namespace to use
        :param timeout: int or float in seconds specifying maximum timeout
            for the operations to last
        :returns: True
        :raises: :class:`asyncio.TimeoutError` if it lasts more than self.timeout
        """
        start = time.monotonic()
        dumps = dumps_fn or self._serializer.dumps
        ns = namespace if namespace is not None else self.namespace

        tmp_pairs = []
        for key, value in pairs:
            tmp_pairs.append((self.build_key(key, namespace=ns), dumps(value)))

        await self._multi_set(tmp_pairs, ttl=self._get_ttl(ttl), _conn=_conn)

        logger.debug(
            "MULTI_SET %s %d (%.4f)s",
            [key for key, value in tmp_pairs],
            len(tmp_pairs),
            time.monotonic() - start,
        )
        return True

    async def _multi_set(self, pairs, ttl, _conn=None):
        raise NotImplementedError()

    @API.register
    @API.aiocache_enabled(fake_return=0)
    @API.timeout
    @API.plugins
    async def delete(self, key, namespace=None, _conn=None):
        """
        Deletes the given key.

        :param key: Key to be deleted
        :param namespace: str alternative namespace to use
        :param timeout: int or float in seconds specifying maximum timeout
            for the operations to last
        :returns: int number of deleted keys
        :raises: :class:`asyncio.TimeoutError` if it lasts more than self.timeout
        """
        start = time.monotonic()
        ns = namespace if namespace is not None else self.namespace
        ns_key = self.build_key(key, namespace=ns)
        ret = await self._delete(ns_key, _conn=_conn)
        logger.debug("DELETE %s %d (%.4f)s", ns_key, ret, time.monotonic() - start)
        return ret

    async def _delete(self, key, _conn=None):
        raise NotImplementedError()

    @API.register
    @API.aiocache_enabled(fake_return=False)
    @API.timeout
    @API.plugins
    async def exists(self, key, namespace=None, _conn=None):
        """
        Check key exists in the cache.

        :param key: str key to check
        :param namespace: str alternative namespace to use
        :param timeout: int or float in seconds specifying maximum timeout
            for the operations to last
        :returns: True if key exists otherwise False
        :raises: :class:`asyncio.TimeoutError` if it lasts more than self.timeout
        """
        start = time.monotonic()
        ns = namespace if namespace is not None else self.namespace
        ns_key = self.build_key(key, namespace=ns)
        ret = await self._exists(ns_key, _conn=_conn)
        logger.debug("EXISTS %s %d (%.4f)s", ns_key, ret, time.monotonic() - start)
        return ret

    async def _exists(self, key, _conn=None):
        raise NotImplementedError()

    @API.register
    @API.aiocache_enabled(fake_return=1)
    @API.timeout
    @API.plugins
    async def increment(self, key, delta=1, namespace=None, _conn=None):
        """
        Increments value stored in key by delta (can be negative). If key doesn't
        exist, it creates the key with delta as value.

        :param key: str key to check
        :param delta: int amount to increment/decrement
        :param namespace: str alternative namespace to use
        :param timeout: int or float in seconds specifying maximum timeout
            for the operations to last
        :returns: Value of the key once incremented. -1 if key is not found.
        :raises: :class:`asyncio.TimeoutError` if it lasts more than self.timeout
        :raises: :class:`TypeError` if value is not incrementable
        """
        start = time.monotonic()
        ns = namespace if namespace is not None else self.namespace
        ns_key = self.build_key(key, namespace=ns)
        ret = await self._increment(ns_key, delta, _conn=_conn)
        logger.debug("INCREMENT %s %d (%.4f)s", ns_key, ret, time.monotonic() - start)
        return ret

    async def _increment(self, key, delta, _conn=None):
        raise NotImplementedError()

    @API.register
    @API.aiocache_enabled(fake_return=False)
    @API.timeout
    @API.plugins
    async def expire(self, key, ttl, namespace=None, _conn=None):
        """
        Set the ttl to the given key. By setting it to 0, it will disable it

        :param key: str key to expire
        :param ttl: int number of seconds for expiration. If 0, ttl is disabled
        :param namespace: str alternative namespace to use
        :param timeout: int or float in seconds specifying maximum timeout
            for the operations to last
        :returns: True if set, False if key is not found
        :raises: :class:`asyncio.TimeoutError` if it lasts more than self.timeout
        """
        start = time.monotonic()
        ns = namespace if namespace is not None else self.namespace
        ns_key = self.build_key(key, namespace=ns)
        ret = await self._expire(ns_key, ttl, _conn=_conn)
        logger.debug("EXPIRE %s %d (%.4f)s", ns_key, ret, time.monotonic() - start)
        return ret

    async def _expire(self, key, ttl, _conn=None):
        raise NotImplementedError()

    @API.register
    @API.aiocache_enabled(fake_return=True)
    @API.timeout
    @API.plugins
    async def clear(self, namespace=None, _conn=None):
        """
        Clears the cache in the cache namespace. If an alternative namespace is given, it will
        clear those ones instead.

        :param namespace: str alternative namespace to use
        :param timeout: int or float in seconds specifying maximum timeout
            for the operations to last
        :returns: True
        :raises: :class:`asyncio.TimeoutError` if it lasts more than self.timeout
        """
        start = time.monotonic()
        ret = await self._clear(namespace, _conn=_conn)
        logger.debug("CLEAR %s %d (%.4f)s", namespace, ret, time.monotonic() - start)
        return ret

    async def _clear(self, namespace, _conn=None):
        raise NotImplementedError()

    @API.register
    @API.aiocache_enabled()
    @API.timeout
    @API.plugins
    async def raw(self, command, *args, _conn=None, **kwargs):
        """
        Send the raw command to the underlying client. Note that by using this CMD you
        will lose compatibility with other backends.

        Due to limitations with aiomcache client, args have to be provided as bytes.
        For rest of backends, str.

        :param command: str with the command.
        :param timeout: int or float in seconds specifying maximum timeout
            for the operations to last
        :returns: whatever the underlying client returns
        :raises: :class:`asyncio.TimeoutError` if it lasts more than self.timeout
        """
        start = time.monotonic()
        ret = await self._raw(
            command, *args, encoding=self.serializer.encoding, _conn=_conn, **kwargs
        )
        logger.debug("%s (%.4f)s", command, time.monotonic() - start)
        return ret

    async def _raw(self, command, *args, **kwargs):
        raise NotImplementedError()

    async def _redlock_release(self, key, value):
        raise NotImplementedError()

    @API.timeout
    async def close(self, *args, _conn=None, **kwargs):
        """
        Perform any resource clean up necessary to exit the program safely.
        After closing, cmd execution is still possible but you will have to
        close again before exiting.

        :raises: :class:`asyncio.TimeoutError` if it lasts more than self.timeout
        """
        start = time.monotonic()
        ret = await self._close(*args, _conn=_conn, **kwargs)
        logger.debug("CLOSE (%.4f)s", time.monotonic() - start)
        return ret

    async def _close(self, *args, **kwargs):
        pass

    def _build_key(self, key, namespace=None):
        if namespace is not None:
            return "{}{}".format(namespace, _ensure_key(key))
        if self.namespace is not None:
            return "{}{}".format(self.namespace, _ensure_key(key))
        return key

    def _get_ttl(self, ttl):
        return ttl if ttl is not SENTINEL else self.ttl

    def get_connection(self):
        return _Conn(self)

    async def acquire_conn(self):
        return self

    async def release_conn(self, conn):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(
        self, exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException], tb: Optional[TracebackType]
    ) -> None:
        await self.close()


class _Conn:
    def __init__(self, cache):
        self._cache = cache
        self._conn = None

    async def __aenter__(self):
        self._conn = await self._cache.acquire_conn()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self._cache.release_conn(self._conn)

    def __getattr__(self, name):
        return self._cache.__getattribute__(name)

    @classmethod
    def _inject_conn(cls, cmd_name):
        async def _do_inject_conn(self, *args, **kwargs):
            return await getattr(self._cache, cmd_name)(*args, _conn=self._conn, **kwargs)

        return _do_inject_conn


def _ensure_key(key):
    if isinstance(key, Enum):
        return key.value
    else:
        return key


for cmd in API.CMDS:
    setattr(_Conn, cmd.__name__, _Conn._inject_conn(cmd.__name__))
