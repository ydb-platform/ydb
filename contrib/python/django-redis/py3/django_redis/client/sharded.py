import builtins
import re
from collections import OrderedDict
from collections.abc import Iterator
from datetime import datetime
from typing import Any, Optional, Union

from redis import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.typing import KeyT

from django_redis.client.default import DEFAULT_TIMEOUT, DefaultClient
from django_redis.exceptions import ConnectionInterrupted
from django_redis.hash_ring import HashRing
from django_redis.util import CacheKey


class ShardClient(DefaultClient):
    _findhash = re.compile(r".*\{(.*)\}.*", re.I)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if not isinstance(self._server, (list, tuple)):
            self._server = [self._server]

        self._ring = HashRing(self._server)
        self._serverdict = self.connect()

    def get_client(self, *args, **kwargs):
        raise NotImplementedError

    def connect(self, index=0):
        connection_dict = {}
        for name in self._server:
            connection_dict[name] = self.connection_factory.connect(name)
        return connection_dict

    def get_server_name(self, _key):
        key = str(_key)
        g = self._findhash.match(key)
        if g is not None and len(g.groups()) > 0:
            key = g.groups()[0]
        return self._ring.get_node(key)

    def get_server(self, key):
        name = self.get_server_name(key)
        return self._serverdict[name]

    def add(self, key, value, timeout=DEFAULT_TIMEOUT, version=None, client=None):
        if client is None:
            key = self.make_key(key, version=version)
            client = self.get_server(key)

        return super().add(
            key=key, value=value, version=version, client=client, timeout=timeout
        )

    def get(self, key, default=None, version=None, client=None):
        if client is None:
            key = self.make_key(key, version=version)
            client = self.get_server(key)

        return super().get(key=key, default=default, version=version, client=client)

    def get_many(self, keys, version=None):
        if not keys:
            return {}

        recovered_data = OrderedDict()

        new_keys = [self.make_key(key, version=version) for key in keys]
        map_keys = dict(zip(new_keys, keys))

        for key in new_keys:
            client = self.get_server(key)
            value = self.get(key=key, version=version, client=client)

            if value is None:
                continue

            recovered_data[map_keys[key]] = value
        return recovered_data

    def set(
        self,
        key,
        value,
        timeout=DEFAULT_TIMEOUT,
        version=None,
        client=None,
        nx=False,
        xx=False,
    ):
        """
        Persist a value to the cache, and set an optional expiration time.
        """
        if client is None:
            key = self.make_key(key, version=version)
            client = self.get_server(key)

        return super().set(
            key=key,
            value=value,
            timeout=timeout,
            version=version,
            client=client,
            nx=nx,
            xx=xx,
        )

    def set_many(self, data, timeout=DEFAULT_TIMEOUT, version=None, client=None):
        """
        Set a bunch of values in the cache at once from a dict of key/value
        pairs. This is much more efficient than calling set() multiple times.

        If timeout is given, that timeout will be used for the key; otherwise
        the default cache timeout will be used.
        """
        for key, value in data.items():
            self.set(key, value, timeout, version=version, client=client)

    def has_key(self, key, version=None, client=None):
        """
        Test if key exists.
        """

        if client is None:
            key = self.make_key(key, version=version)
            client = self.get_server(key)

        key = self.make_key(key, version=version)
        try:
            return client.exists(key) == 1
        except RedisConnectionError as e:
            raise ConnectionInterrupted(connection=client) from e

    def delete(self, key, version=None, client=None):
        if client is None:
            key = self.make_key(key, version=version)
            client = self.get_server(key)

        return super().delete(key=key, version=version, client=client)

    def ttl(self, key, version=None, client=None):
        """
        Executes TTL redis command and return the "time-to-live" of specified key.
        If key is a non volatile key, it returns None.
        """

        if client is None:
            key = self.make_key(key, version=version)
            client = self.get_server(key)

        return super().ttl(key=key, version=version, client=client)

    def pttl(self, key, version=None, client=None):
        """
        Executes PTTL redis command and return the "time-to-live" of specified key
        in milliseconds. If key is a non volatile key, it returns None.
        """

        if client is None:
            key = self.make_key(key, version=version)
            client = self.get_server(key)

        return super().pttl(key=key, version=version, client=client)

    def persist(self, key, version=None, client=None):
        if client is None:
            key = self.make_key(key, version=version)
            client = self.get_server(key)

        return super().persist(key=key, version=version, client=client)

    def expire(self, key, timeout, version=None, client=None):
        if client is None:
            key = self.make_key(key, version=version)
            client = self.get_server(key)

        return super().expire(key=key, timeout=timeout, version=version, client=client)

    def pexpire(self, key, timeout, version=None, client=None):
        if client is None:
            key = self.make_key(key, version=version)
            client = self.get_server(key)

        return super().pexpire(key=key, timeout=timeout, version=version, client=client)

    def pexpire_at(self, key, when: Union[datetime, int], version=None, client=None):
        """
        Set an expire flag on a ``key`` to ``when`` on a shard client.
        ``when`` which can be represented as an integer indicating unix
        time or a Python datetime object.
        """
        if client is None:
            key = self.make_key(key, version=version)
            client = self.get_server(key)

        return super().pexpire_at(key=key, when=when, version=version, client=client)

    def expire_at(self, key, when: Union[datetime, int], version=None, client=None):
        """
        Set an expire flag on a ``key`` to ``when`` on a shard client.
        ``when`` which can be represented as an integer indicating unix
        time or a Python datetime object.
        """
        if client is None:
            key = self.make_key(key, version=version)
            client = self.get_server(key)

        return super().expire_at(key=key, when=when, version=version, client=client)

    def lock(
        self,
        key,
        version=None,
        timeout=None,
        sleep=0.1,
        blocking_timeout=None,
        client=None,
        thread_local=True,
    ):
        if client is None:
            key = self.make_key(key, version=version)
            client = self.get_server(key)

        key = self.make_key(key, version=version)
        return super().lock(
            key,
            timeout=timeout,
            sleep=sleep,
            client=client,
            blocking_timeout=blocking_timeout,
            thread_local=thread_local,
        )

    def delete_many(self, keys, version=None):
        """
        Remove multiple keys at once.
        """
        res = 0
        for key in [self.make_key(k, version=version) for k in keys]:
            client = self.get_server(key)
            res += self.delete(key, client=client)
        return res

    def incr_version(self, key, delta=1, version=None, client=None):
        if client is None:
            key = self.make_key(key, version=version)
            client = self.get_server(key)

        if version is None:
            version = self._backend.version

        old_key = self.make_key(key, version)
        value = self.get(old_key, version=version, client=client)

        try:
            ttl = self.ttl(old_key, version=version, client=client)
        except RedisConnectionError as e:
            raise ConnectionInterrupted(connection=client) from e

        if value is None:
            msg = f"Key '{key}' not found"
            raise ValueError(msg)

        if isinstance(key, CacheKey):
            new_key = self.make_key(key.original_key(), version=version + delta)
        else:
            new_key = self.make_key(key, version=version + delta)

        self.set(new_key, value, timeout=ttl, client=self.get_server(new_key))
        self.delete(old_key, client=client)
        return version + delta

    def incr(self, key, delta=1, version=None, client=None):
        if client is None:
            key = self.make_key(key, version=version)
            client = self.get_server(key)

        return super().incr(key=key, delta=delta, version=version, client=client)

    def decr(self, key, delta=1, version=None, client=None):
        if client is None:
            key = self.make_key(key, version=version)
            client = self.get_server(key)

        return super().decr(key=key, delta=delta, version=version, client=client)

    def iter_keys(self, key, version=None):
        error_message = "iter_keys not supported on sharded client"
        raise NotImplementedError(error_message)

    def keys(self, search, version=None):
        pattern = self.make_pattern(search, version=version)
        keys = []
        try:
            for connection in self._serverdict.values():
                keys.extend(connection.keys(pattern))
        except RedisConnectionError as e:
            # FIXME: technically all clients should be passed as `connection`.
            client = self.get_server(pattern)
            raise ConnectionInterrupted(connection=client) from e

        return [self.reverse_key(k.decode()) for k in keys]

    def delete_pattern(
        self, pattern, version=None, client=None, itersize=None, prefix=None
    ):
        """
        Remove all keys matching pattern.
        """
        pattern = self.make_pattern(pattern, version=version, prefix=prefix)
        kwargs = {"match": pattern}
        if itersize:
            kwargs["count"] = itersize

        keys = []
        for connection in self._serverdict.values():
            keys.extend(key for key in connection.scan_iter(**kwargs))

        res = 0
        if keys:
            for connection in self._serverdict.values():
                res += connection.delete(*keys)
        return res

    def do_close_clients(self):
        for client in self._serverdict.values():
            self.disconnect(client=client)

    def touch(self, key, timeout=DEFAULT_TIMEOUT, version=None, client=None):
        if client is None:
            key = self.make_key(key, version=version)
            client = self.get_server(key)

        return super().touch(key=key, timeout=timeout, version=version, client=client)

    def clear(self, client=None):
        for connection in self._serverdict.values():
            connection.flushdb()

    def sadd(
        self,
        key: KeyT,
        *values: Any,
        version: Optional[int] = None,
        client: Optional[Redis] = None,
    ) -> int:
        if client is None:
            key = self.make_key(key, version=version)
            client = self.get_server(key)
        return super().sadd(key, *values, version=version, client=client)

    def scard(
        self,
        key: KeyT,
        version: Optional[int] = None,
        client: Optional[Redis] = None,
    ) -> int:
        if client is None:
            key = self.make_key(key, version=version)
            client = self.get_server(key)
        return super().scard(key=key, version=version, client=client)

    def smembers(
        self,
        key: KeyT,
        version: Optional[int] = None,
        client: Optional[Redis] = None,
    ) -> builtins.set[Any]:
        if client is None:
            key = self.make_key(key, version=version)
            client = self.get_server(key)
        return super().smembers(key=key, version=version, client=client)

    def smove(
        self,
        source: KeyT,
        destination: KeyT,
        member: Any,
        version: Optional[int] = None,
        client: Optional[Redis] = None,
    ):
        if client is None:
            source = self.make_key(source, version=version)
            client = self.get_server(source)
            destination = self.make_key(destination, version=version)

        return super().smove(
            source=source,
            destination=destination,
            member=member,
            version=version,
            client=client,
        )

    def srem(
        self,
        key: KeyT,
        *members,
        version: Optional[int] = None,
        client: Optional[Redis] = None,
    ) -> int:
        if client is None:
            key = self.make_key(key, version=version)
            client = self.get_server(key)
        return super().srem(key, *members, version=version, client=client)

    def sscan(
        self,
        key: KeyT,
        match: Optional[str] = None,
        count: Optional[int] = 10,
        version: Optional[int] = None,
        client: Optional[Redis] = None,
    ) -> builtins.set[Any]:
        if client is None:
            key = self.make_key(key, version=version)
            client = self.get_server(key)
        return super().sscan(
            key=key, match=match, count=count, version=version, client=client
        )

    def sscan_iter(
        self,
        key: KeyT,
        match: Optional[str] = None,
        count: Optional[int] = 10,
        version: Optional[int] = None,
        client: Optional[Redis] = None,
    ) -> Iterator[Any]:
        if client is None:
            key = self.make_key(key, version=version)
            client = self.get_server(key)
        return super().sscan_iter(
            key=key, match=match, count=count, version=version, client=client
        )

    def srandmember(
        self,
        key: KeyT,
        count: Optional[int] = None,
        version: Optional[int] = None,
        client: Optional[Redis] = None,
    ) -> Union[builtins.set, Any]:
        if client is None:
            key = self.make_key(key, version=version)
            client = self.get_server(key)
        return super().srandmember(key=key, count=count, version=version, client=client)

    def sismember(
        self,
        key: KeyT,
        member: Any,
        version: Optional[int] = None,
        client: Optional[Redis] = None,
    ) -> bool:
        if client is None:
            key = self.make_key(key, version=version)
            client = self.get_server(key)
        return super().sismember(key, member, version=version, client=client)

    def spop(
        self,
        key: KeyT,
        count: Optional[int] = None,
        version: Optional[int] = None,
        client: Optional[Redis] = None,
    ) -> Union[builtins.set, Any]:
        if client is None:
            key = self.make_key(key, version=version)
            client = self.get_server(key)
        return super().spop(key=key, count=count, version=version, client=client)

    def smismember(
        self,
        key: KeyT,
        *members,
        version: Optional[int] = None,
        client: Optional[Redis] = None,
    ) -> list[bool]:
        if client is None:
            key = self.make_key(key, version=version)
            client = self.get_server(key)
        return super().smismember(key, *members, version=version, client=client)
