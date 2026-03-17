"""
    flask_caching.backends.rediscache
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    The redis caching backend.

    :copyright: (c) 2018 by Peter Justin.
    :copyright: (c) 2010 by Thadeus Burgess.
    :license: BSD, see LICENSE for more details.
"""

import pickle

from cachelib import RedisCache as CachelibRedisCache

from flask_caching.backends.base import BaseCache


class RedisCache(BaseCache, CachelibRedisCache):
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
        BaseCache.__init__(self, default_timeout=default_timeout)
        CachelibRedisCache.__init__(
            self,
            host=host,
            port=port,
            password=password,
            db=db,
            default_timeout=default_timeout,
            key_prefix=key_prefix,
            **kwargs
        )

    @classmethod
    def factory(cls, app, config, args, kwargs):
        try:
            from redis import from_url as redis_from_url
        except ImportError as e:
            raise RuntimeError("no redis module found") from e

        kwargs.update(
            dict(
                host=config.get("CACHE_REDIS_HOST", "localhost"),
                port=config.get("CACHE_REDIS_PORT", 6379),
                db=config.get("CACHE_REDIS_DB", 0),
            )
        )
        password = config.get("CACHE_REDIS_PASSWORD")
        if password:
            kwargs["password"] = password

        key_prefix = config.get("CACHE_KEY_PREFIX")
        if key_prefix:
            kwargs["key_prefix"] = key_prefix

        redis_url = config.get("CACHE_REDIS_URL")
        if redis_url:
            kwargs["host"] = redis_from_url(redis_url, db=kwargs.pop("db", None))

        new_class = cls(*args, **kwargs)

        return new_class

    def dump_object(self, value):
        """Dumps an object into a string for redis.  By default it serializes
        integers as regular string and pickle dumps everything else.
        """
        t = type(value)
        if t == int:
            return str(value).encode("ascii")
        return b"!" + pickle.dumps(value)

    def unlink(self, *keys):
        """when redis-py >= 3.0.0 and redis > 4, support this operation"""
        if not keys:
            return
        if self.key_prefix:
            keys = [self.key_prefix + key for key in keys]

        unlink = getattr(self._write_client, "unlink", None)
        if unlink is not None and callable(unlink):
            return self._write_client.unlink(*keys)
        return self._write_client.delete(*keys)


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
        key_prefix="",
        **kwargs
    ):
        super().__init__(key_prefix=key_prefix, default_timeout=default_timeout)

        try:
            import redis.sentinel
        except ImportError as e:
            raise RuntimeError("no redis module found") from e

        if kwargs.get("decode_responses", None):
            raise ValueError("decode_responses is not supported by RedisCache.")

        sentinels = sentinels or [("127.0.0.1", 26379)]
        sentinel_kwargs = {
            key[9:]: value
            for key, value in kwargs.items()
            if key.startswith("sentinel_")
        }
        kwargs = {
            key: value
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
        self._read_client = sentinel.slave_for(master)

    @classmethod
    def factory(cls, app, config, args, kwargs):
        kwargs.update(
            dict(
                sentinels=config.get("CACHE_REDIS_SENTINELS", [("127.0.0.1", 26379)]),
                master=config.get("CACHE_REDIS_SENTINEL_MASTER", "mymaster"),
                password=config.get("CACHE_REDIS_PASSWORD", None),
                sentinel_password=config.get("CACHE_REDIS_SENTINEL_PASSWORD", None),
                key_prefix=config.get("CACHE_KEY_PREFIX", None),
                db=config.get("CACHE_REDIS_DB", 0),
            )
        )

        return cls(*args, **kwargs)


class RedisClusterCache(RedisCache):
    """Uses the Redis key-value store as a cache backend.

    The first argument can be either a string denoting address of the Redis
    server or an object resembling an instance of a rediscluster.RedisCluster
    class.

    Note: Python Redis API already takes care of encoding unicode strings on
    the fly.


    :param cluster: The redis cluster nodes address separated by comma.
                    e.g. host1:port1,host2:port2,host3:port3 .
    :param password: password authentication for the Redis server.
    :param default_timeout: the default timeout that is used if no timeout is
                            specified on :meth:`~BaseCache.set`. A timeout of
                            0 indicates that the cache never expires.
    :param key_prefix: A prefix that should be added to all keys.

    Any additional keyword arguments will be passed to
    ``rediscluster.RedisCluster``.
    """

    def __init__(
        self, cluster="", password="", default_timeout=300, key_prefix="", **kwargs
    ):
        super().__init__(key_prefix=key_prefix, default_timeout=default_timeout)

        if kwargs.get("decode_responses", None):
            raise ValueError("decode_responses is not supported by RedisCache.")

        try:
            from redis import RedisCluster
            from redis.cluster import ClusterNode
        except ImportError as e:
            raise RuntimeError("no redis.cluster module found") from e

        try:
            nodes = [(node.split(":")) for node in cluster.split(",")]
            startup_nodes = [
                ClusterNode(node[0].strip(), node[1].strip()) for node in nodes
            ]
        except IndexError as e:
            raise ValueError(
                "Please give the correct cluster argument "
                "e.g. host1:port1,host2:port2,host3:port3"
            ) from e

        # Skips the check of cluster-require-full-coverage config,
        # useful for clusters without the CONFIG command (like aws)
        skip_full_coverage_check = kwargs.pop("skip_full_coverage_check", True)

        cluster = RedisCluster(
            startup_nodes=startup_nodes,
            password=password,
            skip_full_coverage_check=skip_full_coverage_check,
            **kwargs
        )

        self._write_client = cluster
        self._read_client = cluster

    @classmethod
    def factory(cls, app, config, args, kwargs):
        kwargs.update(
            dict(
                cluster=config.get("CACHE_REDIS_CLUSTER", ""),
                password=config.get("CACHE_REDIS_PASSWORD", ""),
                default_timeout=config.get("CACHE_DEFAULT_TIMEOUT", 300),
                key_prefix=config.get("CACHE_KEY_PREFIX", ""),
            )
        )
        return cls(*args, **kwargs)
