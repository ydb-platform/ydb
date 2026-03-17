from flask_caching._compat import integer_types, string_types
from flask_caching.backends.base import BaseCache, iteritems_wrapper

try:
    import cPickle as pickle
except ImportError:  # pragma: no cover
    import pickle


class RedisCache(BaseCache):
    """Uses the Redis key-value store as a cache backend.

    The first argument can be either a string denoting address of the Redis
    server or an object resembling an instance of a redis.Redis class.

    Note: Python Redis API already takes care of encoding unicode strings on
    the fly.

    :param host: address of the Redis server or an object which API is
                 compatible with the official Python Redis client (redis-py).
    :param port: port number on which Redis server listens for connections.
    :param password: password authentication for the Redis server.
    :param db: db (zero-based numeric index) on Redis Server to connect.
    :param default_timeout: the default timeout that is used if no timeout is
                            specified on :meth:`~BaseCache.set`. A timeout of
                            0 indicates that the cache never expires.
    :param key_prefix: A prefix that should be added to all keys.

    Any additional keyword arguments will be passed to ``redis.Redis``.
    """

    def __init__(
        self,
        host="localhost",
        port=6379,
        password=None,
        db=0,
        default_timeout=300,
        key_prefix=None,
        **kwargs
    ):
        super(RedisCache, self).__init__(default_timeout)
        if host is None:
            raise ValueError("RedisCache host parameter may not be None")
        if isinstance(host, string_types):
            try:
                import redis
            except ImportError:
                raise RuntimeError("no redis module found")
            if kwargs.get("decode_responses", None):
                raise ValueError(
                    "decode_responses is not supported by " "RedisCache."
                )
            client = redis.Redis(
                host=host, port=port, password=password, db=db, **kwargs
            )
        else:
            client = host

        self._write_client = self._read_clients = client
        self.key_prefix = key_prefix or ""

    def _normalize_timeout(self, timeout):
        timeout = BaseCache._normalize_timeout(self, timeout)
        if timeout == 0:
            timeout = -1
        return timeout

    def dump_object(self, value):
        """Dumps an object into a string for redis.  By default it serializes
        integers as regular string and pickle dumps everything else.
        """
        t = type(value)
        if t in integer_types:
            return str(value).encode("ascii")
        return b"!" + pickle.dumps(value)

    def load_object(self, value):
        """The reversal of :meth:`dump_object`.  This might be called with
        None.
        """
        if value is None:
            return None
        if value.startswith(b"!"):
            try:
                return pickle.loads(value[1:])
            except pickle.PickleError:
                return None
        try:
            return int(value)
        except ValueError:
            # before 0.8 we did not have serialization.  Still support that.
            return value

    def get(self, key):
        return self.load_object(self._read_clients.get(self.key_prefix + key))

    def get_many(self, *keys):
        if self.key_prefix:
            keys = [self.key_prefix + key for key in keys]
        return [self.load_object(x) for x in self._read_clients.mget(keys)]

    def set(self, key, value, timeout=None):
        timeout = self._normalize_timeout(timeout)
        dump = self.dump_object(value)
        if timeout == -1:
            result = self._write_client.set(
                name=self.key_prefix + key, value=dump
            )
        else:
            result = self._write_client.setex(
                name=self.key_prefix + key, value=dump, time=timeout
            )
        return result

    def add(self, key, value, timeout=None):
        timeout = self._normalize_timeout(timeout)
        dump = self.dump_object(value)
        return self._write_client.setnx(
            name=self.key_prefix + key, value=dump
        ) and self._write_client.expire(
            name=self.key_prefix + key, time=timeout
        )

    def set_many(self, mapping, timeout=None):
        timeout = self._normalize_timeout(timeout)
        # Use transaction=False to batch without calling redis MULTI
        # which is not supported by twemproxy
        pipe = self._write_client.pipeline(transaction=False)

        for key, value in iteritems_wrapper(mapping):
            dump = self.dump_object(value)
            if timeout == -1:
                pipe.set(name=self.key_prefix + key, value=dump)
            else:
                pipe.setex(name=self.key_prefix + key, value=dump, time=timeout)
        return pipe.execute()

    def delete(self, key):
        return self._write_client.delete(self.key_prefix + key)

    def delete_many(self, *keys):
        if not keys:
            return
        if self.key_prefix:
            keys = [self.key_prefix + key for key in keys]
        return self._write_client.delete(*keys)

    def has(self, key):
        return self._read_clients.exists(self.key_prefix + key)

    def clear(self):
        status = False
        if self.key_prefix:
            keys = self._read_clients.keys(self.key_prefix + "*")
            if keys:
                status = self._write_client.delete(*keys)
        else:
            status = self._write_client.flushdb()
        return status

    def inc(self, key, delta=1):
        return self._write_client.incr(name=self.key_prefix + key, amount=delta)

    def dec(self, key, delta=1):
        return self._write_client.decr(name=self.key_prefix + key, amount=delta)


class RedisSentinelCache(RedisCache):
    """Uses the Redis key-value store as a cache backend.

    The first argument can be either a string denoting address of the Redis
    server or an object resembling an instance of a redis.Redis class.

    Note: Python Redis API already takes care of encoding unicode strings on
    the fly.


    :param sentinels: A list or a tuple of Redis sentinel addresses.
    :param master: The name of the master server in a sentinel configuration.
    :param password: password authentication for the Redis server.
    :param db: db (zero-based numeric index) on Redis Server to connect.
    :param default_timeout: the default timeout that is used if no timeout is
                            specified on :meth:`~BaseCache.set`. A timeout of
                            0 indicates that the cache never expires.
    :param key_prefix: A prefix that should be added to all keys.

    Any additional keyword arguments will be passed to
    ``redis.sentinel.Sentinel``.
    """

    def __init__(
        self,
        sentinels=None,
        master=None,
        password=None,
        db=0,
        default_timeout=300,
        key_prefix=None,
        **kwargs
    ):
        super(RedisSentinelCache, self).__init__(default_timeout)

        try:
            import redis.sentinel
        except ImportError:
            raise RuntimeError("no redis module found")

        if kwargs.get("decode_responses", None):
            raise ValueError(
                "decode_responses is not supported by " "RedisCache."
            )

        sentinels = sentinels or [("127.0.0.1", 26379)]
        sentinel_kwargs = {
            key[9:]: value
            for key, value in kwargs.items()
            if key.startswith("sentinel_")
        }
        kwargs = {
            key[9:]: value
            for key, value in kwargs.items()
            if not key.startswith("sentinel_")
        }

        sentinel = redis.sentinel.Sentinel(
            sentinels=sentinels,
            password=password,
            db=db,
            sentinel_kwargs=sentinel_kwargs,
            **kwargs
        )

        self._write_client = sentinel.master_for(master)
        self._read_clients = sentinel.slave_for(master)

        self.key_prefix = key_prefix or ""
