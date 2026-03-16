"""
    flask_caching.backends.null
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~

    The null cache backend. A caching backend that doesn't cache.

    :copyright: (c) 2018 by Peter Justin.
    :copyright: (c) 2010 by Thadeus Burgess.
    :license: BSD, see LICENSE for more details.
"""

from flask_caching.backends.base import BaseCache


class NullCache(BaseCache):
    """A cache that doesn't cache. This can be useful for unit testing.

    :param default_timeout: a dummy parameter that is ignored but exists
                            for API compatibility with other caches.
    """

    def has(self, key):
        return False
