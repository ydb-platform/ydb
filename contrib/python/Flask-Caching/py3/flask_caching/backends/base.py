"""
    flask_caching.backends.base
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~

    This module contains the BaseCache that other caching
    backends have to implement.

    :copyright: (c) 2018 by Peter Justin.
    :copyright: (c) 2010 by Thadeus Burgess.
    :license: BSD, see LICENSE for more details.
"""

from cachelib import BaseCache as CachelibBaseCache


class BaseCache(CachelibBaseCache):
    """Baseclass for the cache systems.  All the cache systems implement this
    API or a superset of it.

    :param default_timeout: The default timeout (in seconds) that is used if
                            no timeout is specified on :meth:`set`. A timeout
                            of 0 indicates that the cache never expires.
    """

    def __init__(self, default_timeout=300):
        CachelibBaseCache.__init__(self, default_timeout=default_timeout)
        self.ignore_errors = False

    @classmethod
    def factory(cls, app, config, args, kwargs):
        return cls()

    def delete_many(self, *keys):
        """Deletes multiple keys at once.

        :param keys: The function accepts multiple keys as positional
                        arguments.
        :returns: A list containing all sucessfuly deleted keys
        :rtype: boolean
        """
        deleted_keys = []
        for key in keys:
            if self.delete(key):
                deleted_keys.append(key)
            else:
                if not self.ignore_errors:
                    break
        return deleted_keys
