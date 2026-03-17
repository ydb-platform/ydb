"""
    flask_caching.backends.simple
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    The simple cache backend.

    :copyright: (c) 2018 by Peter Justin.
    :copyright: (c) 2010 by Thadeus Burgess.
    :license: BSD, see LICENSE for more details.
"""

import logging

from cachelib import SimpleCache as CachelibSimpleCache

from flask_caching.backends.base import BaseCache


logger = logging.getLogger(__name__)


class SimpleCache(BaseCache, CachelibSimpleCache):
    """Simple memory cache for single process environments. This class exists
    mainly for the development server and is not 100% thread safe.  It tries
    to use as many atomic operations as possible and no locks for simplicity
    but it could happen under heavy load that keys are added multiple times.

    :param threshold: the maximum number of items the cache stores before
                      it starts deleting some.
    :param default_timeout: the default timeout that is used if no timeout is
                            specified on :meth:`~BaseCache.set`. A timeout of
                            0 indicates that the cache never expires.
    :param ignore_errors: If set to ``True`` the :meth:`~BaseCache.delete_many`
                          method will ignore any errors that occurred during
                          the deletion process. However, if it is set to
                          ``False`` it will stop on the first error. Defaults
                          to ``False``.
    """

    def __init__(self, threshold=500, default_timeout=300, ignore_errors=False):
        BaseCache.__init__(self, default_timeout=default_timeout)
        CachelibSimpleCache.__init__(
            self, threshold=threshold, default_timeout=default_timeout
        )

        self.ignore_errors = ignore_errors

    @classmethod
    def factory(cls, app, config, args, kwargs):
        kwargs.update(
            dict(
                threshold=config["CACHE_THRESHOLD"],
                ignore_errors=config["CACHE_IGNORE_ERRORS"],
            )
        )
        return cls(*args, **kwargs)
