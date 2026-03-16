"""
    flask_caching.backends.memcache
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    The memcache caching backend.

    :copyright: (c) 2018 by Peter Justin.
    :copyright: (c) 2010 by Thadeus Burgess.
    :license: BSD, see LICENSE for more details.
"""

import pickle
import re

from cachelib import MemcachedCache as CachelibMemcachedCache

from flask_caching.backends.base import BaseCache


_test_memcached_key = re.compile(r"[^\x00-\x21\xff]{1,250}$").match


class MemcachedCache(BaseCache, CachelibMemcachedCache):
    """A cache that uses memcached as backend.

    The first argument can either be an object that resembles the API of a
    :class:`memcache.Client` or a tuple/list of server addresses. In the
    event that a tuple/list is passed, Werkzeug tries to import the best
    available memcache library.

    This cache looks into the following packages/modules to find bindings for
    memcached:

        - ``pylibmc``
        - ``google.appengine.api.memcached``
        - ``memcached``
        - ``libmc``

    Implementation notes:  This cache backend works around some limitations in
    memcached to simplify the interface.  For example unicode keys are encoded
    to utf-8 on the fly.  Methods such as :meth:`~BaseCache.get_dict` return
    the keys in the same format as passed.  Furthermore all get methods
    silently ignore key errors to not cause problems when untrusted user data
    is passed to the get methods which is often the case in web applications.

    :param servers: a list or tuple of server addresses or alternatively
                    a :class:`memcache.Client` or a compatible client.
    :param default_timeout: the default timeout that is used if no timeout is
                            specified on :meth:`~BaseCache.set`. A timeout of
                            0 indicates that the cache never expires.
    :param key_prefix: a prefix that is added before all keys.  This makes it
                       possible to use the same memcached server for different
                       applications.  Keep in mind that
                       :meth:`~BaseCache.clear` will also clear keys with a
                       different prefix.
    """

    def __init__(self, servers=None, default_timeout=300, key_prefix=None):
        BaseCache.__init__(self, default_timeout=default_timeout)
        CachelibMemcachedCache.__init__(
            self,
            servers=servers,
            default_timeout=default_timeout,
            key_prefix=key_prefix,
        )

    @classmethod
    def factory(cls, app, config, args, kwargs):
        args.append(config["CACHE_MEMCACHED_SERVERS"])
        kwargs.update(dict(key_prefix=config["CACHE_KEY_PREFIX"]))
        return cls(*args, **kwargs)

    def delete_many(self, *keys):
        new_keys = []
        for key in keys:
            key = self._normalize_key(key)
            if _test_memcached_key(key):
                new_keys.append(key)
        return self._client.delete_multi(new_keys)

    def inc(self, key, delta=1):
        key = self._normalize_key(key)
        return self._client.incr(key, delta)

    def dec(self, key, delta=1):
        key = self._normalize_key(key)
        return self._client.decr(key, delta)


class SASLMemcachedCache(MemcachedCache):
    def __init__(
        self,
        servers=None,
        default_timeout=300,
        key_prefix=None,
        username=None,
        password=None,
        **kwargs,
    ):
        super().__init__(default_timeout=default_timeout)

        if servers is None:
            servers = ["127.0.0.1:11211"]

        import pylibmc

        self._client = pylibmc.Client(
            servers, username=username, password=password, binary=True, **kwargs
        )

        self.key_prefix = key_prefix

    @classmethod
    def factory(cls, app, config, args, kwargs):
        args.append(config["CACHE_MEMCACHED_SERVERS"])
        kwargs.update(
            dict(
                username=config["CACHE_MEMCACHED_USERNAME"],
                password=config["CACHE_MEMCACHED_PASSWORD"],
                key_prefix=config["CACHE_KEY_PREFIX"],
            )
        )
        return cls(*args, **kwargs)


class SpreadSASLMemcachedCache(SASLMemcachedCache):
    """Simple Subclass of SASLMemcached client that will spread the value
    across multiple keys if they are bigger than a given threshold.

    Spreading requires using pickle to store the value, which can significantly
    impact the performance.
    """

    def __init__(self, *args, **kwargs):
        """
        Kwargs:
            chunksize (int): max length of a pickled object that can fit in
                memcached (memcache has an upper limit of 1MB for values,
                default: 1048448)
        """
        self.chunksize = kwargs.get("chunksize", 1048448)
        self.maxchunk = kwargs.get("maxchunk", 32)
        super().__init__(*args, **kwargs)

    @classmethod
    def factory(cls, app, config, args, kwargs):
        args.append(config["CACHE_MEMCACHED_SERVERS"])
        kwargs.update(
            dict(
                username=config.get("CACHE_MEMCACHED_USERNAME"),
                password=config.get("CACHE_MEMCACHED_PASSWORD"),
                key_prefix=config.get("CACHE_KEY_PREFIX"),
            )
        )

        return cls(*args, **kwargs)

    def delete(self, key):
        for skey in self._genkeys(key):
            super().delete(skey)

    def set(self, key, value, timeout=None, chunk=True):
        """Set a value in cache, potentially spreading it across multiple key.

        :param key: The cache key.
        :param value: The value to cache.
        :param timeout: The timeout after which the cache will be invalidated.
        :param chunk: If set to `False`, then spreading across multiple keys
                      is disabled. This can be faster, but it will fail if
                      the value is bigger than the chunks. It requires you
                      to get back the object by specifying that it is not
                      spread.
        """
        if chunk:
            return self._set(key, value, timeout=timeout)
        else:
            return super().set(key, value, timeout=timeout)

    def _set(self, key, value, timeout=None):
        # pickling/unpickling add an overhead,
        # I didn't found a good way to avoid pickling/unpickling if
        # key is smaller than chunksize, because in case or <werkzeug.requests>
        # getting the length consume the data iterator.
        serialized = pickle.dumps(value, 2)
        values = {}
        len_ser = len(serialized)
        chks = range(0, len_ser, self.chunksize)

        if len(chks) > self.maxchunk:
            raise ValueError("Cannot store value in less than %s keys" % self.maxchunk)

        for i in chks:
            values[f"{key}.{i // self.chunksize}"] = serialized[i : i + self.chunksize]

        super().set_many(values, timeout)

    def get(self, key, chunk=True):
        """Get a cached value.

        :param chunk: If set to ``False``, it will return a cached value
                      that is spread across multiple keys.
        """
        if chunk:
            return self._get(key)
        else:
            return super().get(key)

    def _genkeys(self, key):
        return [f"{key}.{i}" for i in range(self.maxchunk)]

    def _get(self, key):
        to_get = [f"{key}.{i}" for i in range(self.maxchunk)]
        result = super().get_many(*to_get)
        serialized = b"".join(v for v in result if v is not None)

        if not serialized:
            return None

        return pickle.loads(serialized)
