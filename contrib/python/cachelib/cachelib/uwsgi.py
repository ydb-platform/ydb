import platform
import typing as _t

from cachelib.base import BaseCache
from cachelib.serializers import UWSGISerializer


class UWSGICache(BaseCache):
    """Implements the cache using uWSGI's caching framework.

    .. note::
        This class cannot be used when running under PyPy, because the uWSGI
        API implementation for PyPy is lacking the needed functionality.

    :param default_timeout: The default timeout in seconds.
    :param cache: The name of the caching instance to connect to, for
        example: mycache@localhost:3031, defaults to an empty string, which
        means uWSGI will cache in the local instance. If the cache is in the
        same instance as the werkzeug app, you only have to provide the name of
        the cache.
    """

    serializer = UWSGISerializer()

    def __init__(
        self,
        default_timeout: int = 300,
        cache: str = "",
    ):
        BaseCache.__init__(self, default_timeout)

        if platform.python_implementation() == "PyPy":
            raise RuntimeError(
                "uWSGI caching does not work under PyPy, see "
                "the docs for more details."
            )

        try:
            import uwsgi  # type: ignore

            self._uwsgi = uwsgi
        except ImportError as err:
            raise RuntimeError(
                "uWSGI could not be imported, are you running under uWSGI?"
            ) from err

        self.cache = cache

    def get(self, key: str) -> _t.Any:
        rv = self._uwsgi.cache_get(key, self.cache)
        if rv is None:
            return
        return self.serializer.loads(rv)

    def delete(self, key: str) -> bool:
        return bool(self._uwsgi.cache_del(key, self.cache))

    def set(
        self, key: str, value: _t.Any, timeout: _t.Optional[int] = None
    ) -> _t.Optional[bool]:
        result = self._uwsgi.cache_update(
            key,
            self.serializer.dumps(value),
            self._normalize_timeout(timeout),
            self.cache,
        )  # type: bool
        return result

    def add(self, key: str, value: _t.Any, timeout: _t.Optional[int] = None) -> bool:
        return bool(
            self._uwsgi.cache_set(
                key,
                self.serializer.dumps(value),
                self._normalize_timeout(timeout),
                self.cache,
            )
        )

    def clear(self) -> bool:
        return bool(self._uwsgi.cache_clear(self.cache))

    def has(self, key: str) -> bool:
        return self._uwsgi.cache_exists(key, self.cache) is not None
