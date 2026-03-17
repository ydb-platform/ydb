from functools import wraps

from django.core.cache.backends.base import (
    BaseCache, DEFAULT_TIMEOUT, InvalidCacheBackendError,
)
from django.core.exceptions import ImproperlyConfigured

try:
    import redis
except ImportError:
    raise InvalidCacheBackendError(
        "Redis cache backend requires the 'redis-py' library"
    )

from redis.connection import DefaultParser
from redis_cache.constants import KEY_EXPIRED, KEY_NON_VOLATILE
from redis_cache.connection import pool
from redis_cache.utils import get_servers, parse_connection_kwargs, import_class


def get_client(write=False):

    def wrapper(method):

        @wraps(method)
        def wrapped(self, key, *args, **kwargs):
            version = kwargs.pop('version', None)
            key = self.make_key(key, version=version)
            client = self.get_client(key, write=write)
            return method(self, client, key, *args, **kwargs)

        return wrapped

    return wrapper


class BaseRedisCache(BaseCache):

    def __init__(self, server, params):
        """
        Connect to Redis, and set up cache backend.
        """
        super(BaseRedisCache, self).__init__(params)
        self.server = server
        self.servers = get_servers(server)
        self.params = params or {}
        self.options = params.get('OPTIONS', {})
        self.clients = {}
        self.client_list = []

        self.db = self.get_db()
        self.password = self.get_password()
        self.parser_class = self.get_parser_class()
        self.pickle_version = self.get_pickle_version()
        self.socket_timeout = self.get_socket_timeout()
        self.socket_connect_timeout = self.get_socket_connect_timeout()
        self.connection_pool_class = self.get_connection_pool_class()
        self.connection_pool_class_kwargs = (
            self.get_connection_pool_class_kwargs()
        )

        # Serializer
        self.serializer_class = self.get_serializer_class()
        self.serializer_class_kwargs = self.get_serializer_class_kwargs()
        self.serializer = self.serializer_class(
            **self.serializer_class_kwargs
        )

        # Compressor
        self.compressor_class = self.get_compressor_class()
        self.compressor_class_kwargs = self.get_compressor_class_kwargs()
        self.compressor = self.compressor_class(
            **self.compressor_class_kwargs
        )

        redis_py_version = tuple(int(part) for part in redis.__version__.split('.'))
        if redis_py_version < (3, 0, 0):
            self.Redis = redis.StrictRedis
        else:
            self.Redis = redis.Redis

    def __getstate__(self):
        return {'params': self.params, 'server': self.server}

    def __setstate__(self, state):
        self.__init__(**state)

    def get_db(self):
        _db = self.params.get('db', self.options.get('DB', 1))
        try:
            return int(_db)
        except (ValueError, TypeError):
            raise ImproperlyConfigured("db value must be an integer")

    def get_password(self):
        return self.params.get('password', self.options.get('PASSWORD', None))

    def get_parser_class(self):
        parser_class = self.options.get('PARSER_CLASS', None)
        if parser_class is None:
            return DefaultParser
        return import_class(parser_class)

    def get_pickle_version(self):
        """
        Get the pickle version from the settings and save it for future use
        """
        _pickle_version = self.options.get('PICKLE_VERSION', -1)
        try:
            return int(_pickle_version)
        except (ValueError, TypeError):
            raise ImproperlyConfigured(
                "pickle version value must be an integer"
            )

    def get_socket_timeout(self):
        return self.options.get('SOCKET_TIMEOUT', None)

    def get_socket_connect_timeout(self):
        return self.options.get('SOCKET_CONNECT_TIMEOUT', None)

    def get_connection_pool_class(self):
        pool_class = self.options.get(
            'CONNECTION_POOL_CLASS',
            'redis.ConnectionPool'
        )
        return import_class(pool_class)

    def get_connection_pool_class_kwargs(self):
        return self.options.get('CONNECTION_POOL_CLASS_KWARGS', {})

    def get_serializer_class(self):
        serializer_class = self.options.get(
            'SERIALIZER_CLASS',
            'redis_cache.serializers.PickleSerializer'
        )
        return import_class(serializer_class)

    def get_serializer_class_kwargs(self):
        kwargs = self.options.get('SERIALIZER_CLASS_KWARGS', {})
        serializer_class = self.options.get(
            'SERIALIZER_CLASS',
            'redis_cache.serializers.PickleSerializer'
        )
        if serializer_class == 'redis_cache.serializers.PickleSerializer':
            kwargs['pickle_version'] = kwargs.get(
                'pickle_version',
                self.pickle_version
            )
        return kwargs

    def get_compressor_class(self):
        compressor_class = self.options.get(
            'COMPRESSOR_CLASS',
            'redis_cache.compressors.NoopCompressor'
        )
        return import_class(compressor_class)

    def get_compressor_class_kwargs(self):
        return self.options.get('COMPRESSOR_CLASS_KWARGS', {})

    def get_master_client(self):
        """
        Get the write server:port of the master cache
        """
        cache = self.options.get('MASTER_CACHE', None)
        if cache is None:
            return next(iter(self.client_list))

        kwargs = parse_connection_kwargs(cache, db=self.db)
        return self.clients[(
            kwargs['host'],
            kwargs['port'],
            kwargs['db'],
            kwargs['unix_socket_path'],
        )]

    def create_client(self, server):
        kwargs = parse_connection_kwargs(
            server,
            db=self.db,
            password=self.password,
            socket_timeout=self.socket_timeout,
            socket_connect_timeout=self.socket_connect_timeout,
        )

        # remove socket-related connection arguments
        if kwargs.get('ssl', False):
            del kwargs['socket_timeout']
            del kwargs['socket_connect_timeout']
            del kwargs['unix_socket_path']

        client = redis.Redis(**kwargs)
        kwargs.update(
            parser_class=self.parser_class,
            connection_pool_class=self.connection_pool_class,
            connection_pool_class_kwargs=self.connection_pool_class_kwargs,
        )
        connection_pool = pool.get_connection_pool(client, **kwargs)
        client.connection_pool = connection_pool
        return client

    def serialize(self, value):
        return self.serializer.serialize(value)

    def deserialize(self, value):
        return self.serializer.deserialize(value)

    def compress(self, value):
        return self.compressor.compress(value)

    def decompress(self, value):
        return self.compressor.decompress(value)

    def get_value(self, original):
        try:
            value = int(original)
        except (ValueError, TypeError):
            value = self.decompress(original)
            value = self.deserialize(value)
        return value

    def prep_value(self, value):
        if isinstance(value, int) and not isinstance(value, bool):
            return value
        value = self.serialize(value)
        return self.compress(value)

    def make_keys(self, keys, version=None):
        return [self.make_key(key, version=version) for key in keys]

    def get_timeout(self, timeout):
        if timeout is DEFAULT_TIMEOUT:
            timeout = self.default_timeout

        if timeout is not None:
            timeout = int(timeout)

        return timeout

    ####################
    # Django cache api #
    ####################

    @get_client(write=True)
    def add(self, client, key, value, timeout=DEFAULT_TIMEOUT):
        """Add a value to the cache, failing if the key already exists.

        Returns ``True`` if the object was added, ``False`` if not.
        """
        timeout = self.get_timeout(timeout)
        return self._set(client, key, self.prep_value(value), timeout, _add_only=True)

    def _get(self, client, key, default=None):
        value = client.get(key)
        if value is None:
            return default
        value = self.get_value(value)
        return value

    @get_client()
    def get(self, client, key, default=None):
        """Retrieve a value from the cache.

        Returns deserialized value if key is found, the default if not.
        """
        return self._get(client, key, default)

    def _set(self, client, key, value, timeout, _add_only=False):
        if timeout is not None and timeout < 0:
            return False
        elif timeout == 0:
            return client.expire(key, 0)
        return client.set(key, value, nx=_add_only, ex=timeout)

    @get_client(write=True)
    def set(self, client, key, value, timeout=DEFAULT_TIMEOUT):
        """Persist a value to the cache, and set an optional expiration time.
        """
        timeout = self.get_timeout(timeout)
        result = self._set(client, key, self.prep_value(value), timeout, _add_only=False)
        return result

    @get_client(write=True)
    def delete(self, client, key):
        """Remove a key from the cache."""
        return client.delete(key)

    def _delete_many(self, client, keys):
        return client.delete(*keys)

    def delete_many(self, keys, version=None):
        """
        Remove multiple keys at once.
        """
        raise NotImplementedError

    def _clear(self, client):
        return client.flushdb()

    def clear(self, version=None):
        """Flush cache keys.

        If version is specified, all keys belonging the version's key
        namespace will be deleted.  Otherwise, all keys will be deleted.
        """
        raise NotImplementedError

    def _get_many(self, client, original_keys, versioned_keys):
        recovered_data = {}
        map_keys = dict(zip(versioned_keys, original_keys))

        # Only try to mget if we actually received any keys to get
        if map_keys:
            results = client.mget(versioned_keys)

            for key, value in zip(versioned_keys, results):
                if value is None:
                    continue
                recovered_data[map_keys[key]] = self.get_value(value)

        return recovered_data

    def get_many(self, keys, version=None):
        """Retrieve many keys."""
        raise NotImplementedError

    def set_many(self, data, timeout=DEFAULT_TIMEOUT, version=None):
        """Set a bunch of values in the cache at once from a dict of key/value
        pairs. This is much more efficient than calling set() multiple times.

        If timeout is given, that timeout will be used for the key; otherwise
        the default cache timeout will be used.
        """
        raise NotImplementedError

    @get_client(write=True)
    def incr(self, client, key, delta=1):
        """Add delta to value in the cache. If the key does not exist, raise a
        `ValueError` exception.
        """
        exists = client.exists(key)
        if not exists:
            raise ValueError("Key '%s' not found" % key)

        value = client.incr(key, delta)

        return value

    def _incr_version(self, client, old, new, original, delta, version):
        try:
            client.rename(old, new)
        except redis.ResponseError:
            raise ValueError("Key '%s' not found" % original)
        return version + delta

    def incr_version(self, key, delta=1, version=None):
        """Adds delta to the cache version for the supplied key. Returns the
        new version.
        """

    @get_client(write=True)
    def touch(self, client, key, timeout=DEFAULT_TIMEOUT):
        """Reset the timeout of a key to `timeout` seconds."""
        return client.expire(key, timeout)

    #####################
    # Extra api methods #
    #####################

    @get_client()
    def has_key(self, client, key):
        """Returns True if the key is in the cache and has not expired."""
        return client.exists(key)

    @get_client()
    def ttl(self, client, key):
        """Returns the 'time-to-live' of a key.  If the key is not volatile,
        i.e. it has not set expiration, then the value returned is None.
        Otherwise, the value is the number of seconds remaining.  If the key
        does not exist, 0 is returned.
        """
        ttl = client.ttl(key)
        if ttl == KEY_NON_VOLATILE:
            return None
        elif ttl == KEY_EXPIRED:
            return 0
        else:
            return ttl

    def _delete_pattern(self, client, pattern):
        keys = list(client.scan_iter(match=pattern))
        if keys:
            client.delete(*keys)

    def delete_pattern(self, pattern, version=None):
        raise NotImplementedError

    def lock(
            self,
            key,
            timeout=None,
            sleep=0.1,
            blocking_timeout=None,
            thread_local=True):
        client = self.get_client(key, write=True)
        return client.lock(
            key,
            timeout=timeout,
            sleep=sleep,
            blocking_timeout=blocking_timeout,
            thread_local=thread_local
        )

    @get_client(write=True)
    def get_or_set(
            self,
            client,
            key,
            default,
            timeout=DEFAULT_TIMEOUT,
            lock_timeout=None,
            stale_cache_timeout=None):
        """Get a value from the cache or use ``default`` to set it and return it.

        If ``default`` is a callable, call it without arguments and store its return value in the cache instead.

        This implementation is slightly more advanced that Django's.  It provides thundering herd
        protection, which prevents multiple threads/processes from calling the value-generating
        function too much.

        There are three timeouts you can specify:

        ``timeout``: Time in seconds that value at ``key`` is considered fresh.
        ``lock_timeout``: Time in seconds that the lock will stay active and prevent other threads
            or processes from acquiring the lock.
        ``stale_cache_timeout``: Time in seconds that the stale cache will remain after the key has
            expired. If ``None`` is specified, the stale value will remain indefinitely.

        """
        lock_key = "__lock__" + key
        fresh_key = "__fresh__" + key

        is_fresh = self._get(client, fresh_key)
        value = self._get(client, key)

        if is_fresh:
            return value

        timeout = self.get_timeout(timeout)
        lock = self.lock(lock_key, timeout=lock_timeout)

        acquired = lock.acquire(blocking=False)

        if acquired:
            try:
                value = default() if callable(default) else default
            except Exception:
                raise
            else:
                key_timeout = (
                    None if stale_cache_timeout is None else timeout + stale_cache_timeout
                )
                pipeline = client.pipeline()
                pipeline.set(key, self.prep_value(value), key_timeout)
                pipeline.set(fresh_key, 1, timeout)
                pipeline.execute()
            finally:
                lock.release()

        return value

    def _reinsert_keys(self, client):
        keys = list(client.scan_iter(match='*'))
        for key in keys:
            timeout = client.ttl(key)
            value = self.get_value(client.get(key))
            if timeout is None:
                client.set(key, self.prep_value(value))

    def reinsert_keys(self):
        """
        Reinsert cache entries using the current pickle protocol version.
        """
        raise NotImplementedError

    @get_client(write=True)
    def persist(self, client, key):
        """Remove the timeout on a key.

        Equivalent to setting a timeout of None in a set command.

        Returns True if successful and False if not.
        """
        return client.persist(key)
