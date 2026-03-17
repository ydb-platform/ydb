import typing as _t


class BaseCache:
    """Baseclass for the cache systems.  All the cache systems implement this
    API or a superset of it.

    :param default_timeout: the default timeout (in seconds) that is used if
                            no timeout is specified on :meth:`set`. A timeout
                            of 0 indicates that the cache never expires.
    """

    def __init__(self, default_timeout: int = 300):
        self.default_timeout = default_timeout

    def _normalize_timeout(self, timeout: _t.Optional[int]) -> int:
        if timeout is None:
            timeout = self.default_timeout
        return timeout

    def get(self, key: str) -> _t.Any:
        """Look up key in the cache and return the value for it.

        :param key: the key to be looked up.
        :returns: The value if it exists and is readable, else ``None``.
        """
        return None

    def delete(self, key: str) -> bool:
        """Delete `key` from the cache.

        :param key: the key to delete.
        :returns: Whether the key existed and has been deleted.
        :rtype: boolean
        """
        return True

    def get_many(self, *keys: str) -> _t.List[_t.Any]:
        """Returns a list of values for the given keys.
        For each key an item in the list is created::

            foo, bar = cache.get_many("foo", "bar")

        Has the same error handling as :meth:`get`.

        :param keys: The function accepts multiple keys as positional
                     arguments.
        """
        return [self.get(k) for k in keys]

    def get_dict(self, *keys: str) -> _t.Dict[str, _t.Any]:
        """Like :meth:`get_many` but return a dict::

            d = cache.get_dict("foo", "bar")
            foo = d["foo"]
            bar = d["bar"]

        :param keys: The function accepts multiple keys as positional
                     arguments.
        """
        return dict(zip(keys, self.get_many(*keys)))  # noqa: B905

    def set(
        self, key: str, value: _t.Any, timeout: _t.Optional[int] = None
    ) -> _t.Optional[bool]:
        """Add a new key/value to the cache (overwrites value, if key already
        exists in the cache).

        :param key: the key to set
        :param value: the value for the key
        :param timeout: the cache timeout for the key in seconds (if not
                        specified, it uses the default timeout). A timeout of
                        0 indicates that the cache never expires.
        :returns: ``True`` if key has been updated, ``False`` for backend
                  errors. Pickling errors, however, will raise a subclass of
                  ``pickle.PickleError``.
        :rtype: boolean
        """
        return True

    def add(self, key: str, value: _t.Any, timeout: _t.Optional[int] = None) -> bool:
        """Works like :meth:`set` but does not overwrite the values of already
        existing keys.

        :param key: the key to set
        :param value: the value for the key
        :param timeout: the cache timeout for the key in seconds (if not
                        specified, it uses the default timeout). A timeout of
                        0 indicates that the cache never expires.
        :returns: Same as :meth:`set`, but also ``False`` for already
                  existing keys.
        :rtype: boolean
        """
        return True

    def set_many(
        self, mapping: _t.Dict[str, _t.Any], timeout: _t.Optional[int] = None
    ) -> _t.List[_t.Any]:
        """Sets multiple keys and values from a mapping.

        :param mapping: a mapping with the keys/values to set.
        :param timeout: the cache timeout for the key in seconds (if not
                        specified, it uses the default timeout). A timeout of
                        0 indicates that the cache never expires.
        :returns: A list containing all keys successfully set
        :rtype: boolean
        """
        set_keys = []
        for key, value in mapping.items():
            if self.set(key, value, timeout):
                set_keys.append(key)
        return set_keys

    def delete_many(self, *keys: str) -> _t.List[_t.Any]:
        """Deletes multiple keys at once.

        :param keys: The function accepts multiple keys as positional
                     arguments.
        :returns: A list containing all successfully deleted keys
        :rtype: boolean
        """
        deleted_keys = []
        for key in keys:
            if self.delete(key):
                deleted_keys.append(key)
        return deleted_keys

    def has(self, key: str) -> bool:
        """Checks if a key exists in the cache without returning it. This is a
        cheap operation that bypasses loading the actual data on the backend.

        :param key: the key to check
        """
        raise NotImplementedError(
            "%s doesn't have an efficient implementation of `has`. That "
            "means it is impossible to check whether a key exists without "
            "fully loading the key's data. Consider using `self.get` "
            "explicitly if you don't care about performance."
        )

    def clear(self) -> bool:
        """Clears the cache.  Keep in mind that not all caches support
        completely clearing the cache.

        :returns: Whether the cache has been cleared.
        :rtype: boolean
        """
        return True

    def inc(self, key: str, delta: int = 1) -> _t.Optional[int]:
        """Increments the value of a key by `delta`.  If the key does
        not yet exist it is initialized with `delta`.

        For supporting caches this is an atomic operation.

        :param key: the key to increment.
        :param delta: the delta to add.
        :returns: The new value or ``None`` for backend errors.
        """
        value = (self.get(key) or 0) + delta
        return value if self.set(key, value) else None

    def dec(self, key: str, delta: int = 1) -> _t.Optional[int]:
        """Decrements the value of a key by `delta`.  If the key does
        not yet exist it is initialized with `-delta`.

        For supporting caches this is an atomic operation.

        :param key: the key to increment.
        :param delta: the delta to subtract.
        :returns: The new value or `None` for backend errors.
        """
        value = (self.get(key) or 0) - delta
        return value if self.set(key, value) else None


class NullCache(BaseCache):
    """A cache that doesn't cache.  This can be useful for unit testing.

    :param default_timeout: a dummy parameter that is ignored but exists
                            for API compatibility with other caches.
    """

    def has(self, key: str) -> bool:
        return False
