"""
    flask_caching.backends.filesystem
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    The filesystem caching backend.

    :copyright: (c) 2018 by Peter Justin.
    :copyright: (c) 2010 by Thadeus Burgess.
    :license: BSD, see LICENSE for more details.
"""

import hashlib
import logging

from cachelib import FileSystemCache as CachelibFileSystemCache

from flask_caching.backends.base import BaseCache

logger = logging.getLogger(__name__)


class FileSystemCache(BaseCache, CachelibFileSystemCache):
    """A cache that stores the items on the file system.  This cache depends
    on being the only user of the `cache_dir`.  Make absolutely sure that
    nobody but this cache stores files there or otherwise the cache will
    randomly delete files therein.

    :param cache_dir: the directory where cache files are stored.
    :param threshold: the maximum number of items the cache stores before
                      it starts deleting some. A threshold value of 0
                      indicates no threshold.
    :param default_timeout: the default timeout that is used if no timeout is
                            specified on :meth:`~BaseCache.set`. A timeout of
                            0 indicates that the cache never expires.
    :param mode: the file mode wanted for the cache files, default 0600
    :param hash_method: Default hashlib.md5. The hash method used to
                        generate the filename for cached results.
    :param ignore_errors: If set to ``True`` the :meth:`~BaseCache.delete_many`
                          method will ignore any errors that occurred during the
                          deletion process. However, if it is set to ``False``
                          it will stop on the first error. Defaults to
                          ``False``.
    """

    def __init__(
        self,
        cache_dir,
        threshold=500,
        default_timeout=300,
        mode=0o600,
        hash_method=hashlib.md5,
        ignore_errors=False,
    ):

        BaseCache.__init__(self, default_timeout=default_timeout)
        CachelibFileSystemCache.__init__(
            self,
            cache_dir=cache_dir,
            threshold=threshold,
            default_timeout=default_timeout,
            mode=mode,
            hash_method=hash_method,
        )

        self.ignore_errors = ignore_errors

    @classmethod
    def factory(cls, app, config, args, kwargs):
        args.insert(0, config["CACHE_DIR"])
        kwargs.update(
            dict(
                threshold=config["CACHE_THRESHOLD"],
                ignore_errors=config["CACHE_IGNORE_ERRORS"],
            )
        )
        return cls(*args, **kwargs)
