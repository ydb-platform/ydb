import re
import typing as _t
from time import time

from cachelib.base import BaseCache


_test_memcached_key = re.compile(r"[^\x00-\x21\xff]{1,250}$").match


class MemcachedCache(BaseCache):
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

    def __init__(
        self,
        servers: _t.Any = None,
        default_timeout: int = 300,
        key_prefix: _t.Optional[str] = None,
    ):
        BaseCache.__init__(self, default_timeout)
        if servers is None or isinstance(servers, (list, tuple)):
            if servers is None:
                servers = ["127.0.0.1:11211"]
            self._client = self.import_preferred_memcache_lib(servers)
            if self._client is None:
                raise RuntimeError("no memcache module found")
        else:
            # NOTE: servers is actually an already initialized memcache
            # client.
            self._client = servers

        self.key_prefix = key_prefix

    def _normalize_key(self, key: str) -> str:
        if self.key_prefix:
            key = self.key_prefix + key
        return key

    def _normalize_timeout(self, timeout: _t.Optional[int]) -> int:
        timeout = BaseCache._normalize_timeout(self, timeout)
        if timeout > 0:
            timeout = int(time()) + timeout
        return timeout

    def get(self, key: str) -> _t.Any:
        key = self._normalize_key(key)
        # memcached doesn't support keys longer than that.  Because often
        # checks for so long keys can occur because it's tested from user
        # submitted data etc we fail silently for getting.
        if _test_memcached_key(key):
            return self._client.get(key)

    def get_dict(self, *keys: str) -> _t.Dict[str, _t.Any]:
        key_mapping = {}
        for key in keys:
            encoded_key = self._normalize_key(key)
            if _test_memcached_key(key):
                key_mapping[encoded_key] = key
        _keys = list(key_mapping)
        d = rv = self._client.get_multi(_keys)  # type: _t.Dict[str, _t.Any]
        if self.key_prefix:
            rv = {}
            for key, value in d.items():
                rv[key_mapping[key]] = value
        if len(rv) < len(keys):
            for key in keys:
                if key not in rv:
                    rv[key] = None
        return rv

    def add(self, key: str, value: _t.Any, timeout: _t.Optional[int] = None) -> bool:
        key = self._normalize_key(key)
        timeout = self._normalize_timeout(timeout)
        return bool(self._client.add(key, value, timeout))

    def set(
        self, key: str, value: _t.Any, timeout: _t.Optional[int] = None
    ) -> _t.Optional[bool]:
        key = self._normalize_key(key)
        timeout = self._normalize_timeout(timeout)
        return bool(self._client.set(key, value, timeout))

    def get_many(self, *keys: str) -> _t.List[_t.Any]:
        d = self.get_dict(*keys)
        return [d[key] for key in keys]

    def set_many(
        self, mapping: _t.Dict[str, _t.Any], timeout: _t.Optional[int] = None
    ) -> _t.List[_t.Any]:
        new_mapping = {}
        for key, value in mapping.items():
            key = self._normalize_key(key)
            new_mapping[key] = value

        timeout = self._normalize_timeout(timeout)
        failed_keys = self._client.set_multi(
            new_mapping, timeout
        )  # type: _t.List[_t.Any]
        k_normkey = zip(mapping.keys(), new_mapping.keys())  # noqa: B905
        return [k for k, nkey in k_normkey if nkey not in failed_keys]

    def delete(self, key: str) -> bool:
        key = self._normalize_key(key)
        if _test_memcached_key(key):
            return bool(self._client.delete(key))
        return False

    def delete_many(self, *keys: str) -> _t.List[_t.Any]:
        new_keys = []
        for key in keys:
            key = self._normalize_key(key)
            if _test_memcached_key(key):
                new_keys.append(key)
        self._client.delete_multi(new_keys)
        return [k for k in new_keys if not self.has(k)]

    def has(self, key: str) -> bool:
        key = self._normalize_key(key)
        if _test_memcached_key(key):
            return bool(self._client.append(key, ""))
        return False

    def clear(self) -> bool:
        return bool(self._client.flush_all())

    def inc(self, key: str, delta: int = 1) -> _t.Optional[int]:
        key = self._normalize_key(key)
        value = (self._client.get(key) or 0) + delta
        return value if self.set(key, value) else None

    def dec(self, key: str, delta: int = 1) -> _t.Optional[int]:
        key = self._normalize_key(key)
        value = (self._client.get(key) or 0) - delta
        return value if self.set(key, value) else None

    def import_preferred_memcache_lib(self, servers: _t.Any) -> _t.Any:
        """Returns an initialized memcache client.  Used by the constructor."""
        try:
            import pylibmc  # type: ignore
        except ImportError:
            pass
        else:
            return pylibmc.Client(servers)

        try:
            from google.appengine.api import memcache  # type: ignore
        except ImportError:
            pass
        else:
            return memcache.Client()

        try:
            import memcache  # type: ignore
        except ImportError:
            pass
        else:
            return memcache.Client(servers)

        try:
            import libmc  # type: ignore
        except ImportError:
            pass
        else:
            return libmc.Client(servers)
