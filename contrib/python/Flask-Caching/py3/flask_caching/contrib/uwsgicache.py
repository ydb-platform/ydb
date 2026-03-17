"""
    flask_caching.backends.uwsgicache
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    The uWSGI caching backend.

    :copyright: (c) 2018 by Peter Justin.
    :copyright: (c) 2010 by Thadeus Burgess.
    :license: BSD, see LICENSE for more details.
"""

from cachelib import UWSGICache as CachelibUWSGICache

from flask_caching.backends.base import BaseCache


class UWSGICache(BaseCache, CachelibUWSGICache):
    """Implements the cache using uWSGI's caching framework.

    .. note::
        This class cannot be used when running under PyPy, because the uWSGI
        API implementation for PyPy is lacking the needed functionality.

    :param default_timeout: The default timeout in seconds.
    :param cache: The name of the caching instance to connect to, for
        example: mycache@localhost:3031, defaults to an empty string, which
        means uWSGI will use the first cache instance initialized.
        If the cache is in the same instance as the werkzeug app,
        you only have to provide the name of the cache.
    """

    def __init__(self, default_timeout=300, cache=""):
        BaseCache.__init__(self, default_timeout=default_timeout)
        CachelibUWSGICache.__init__(
            self,
            cache=cache,
            default_timeout=default_timeout,
        )

        try:
            import uwsgi

            self._uwsgi = uwsgi
        except ImportError as e:
            raise RuntimeError(
                "uWSGI could not be imported, are you running under uWSGI?"
            ) from e

        if "cache2" not in uwsgi.opt:
            raise RuntimeError(
                "You must enable cache2 in uWSGI configuration: "
                "https://uwsgi-docs.readthedocs.io/en/latest/Caching.html"
            )

    @classmethod
    def factory(cls, app, config, args, kwargs):
        # The name of the caching instance to connect to, for
        # example: mycache@localhost:3031, defaults to an empty string, which
        # means uWSGI will cache in the local instance. If the cache is in the
        # same instance as the werkzeug app, you only have to provide the name
        # of the cache.
        uwsgi_cache_name = config.get("CACHE_UWSGI_NAME", "")
        kwargs.update(dict(cache=uwsgi_cache_name))
        return cls(*args, **kwargs)
