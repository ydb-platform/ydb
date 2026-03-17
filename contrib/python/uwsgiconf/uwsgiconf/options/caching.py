from pathlib import Path
from typing import Union

from ..base import OptionsGroup
from ..typehints import Strlist
from ..utils import KeyValue


class Caching(OptionsGroup):
    """Caching.

    uWSGI includes a very fast, all-in-memory, zero-IPC, SMP-safe, constantly-optimizing,
    highly-tunable, key-value store simply called "the caching framework".

    A single uWSGI instance can create an unlimited number of "caches" each one
    with different setup and purpose.

    * http://uwsgi-docs.readthedocs.io/en/latest/Caching.html
    * http://uwsgi-docs.readthedocs.io/en/latest/tutorials/CachingCookbook.html

    """
    def set_basic_params(
            self,
            *,
            no_expire: bool = None,
            expire_scan_interval: int = None,
            report_freed: bool = None
    ):
        """
        :param no_expire: Disable auto sweep of expired items.
            Since uWSGI 1.2, cache item expiration is managed by a thread in the master process,
            to reduce the risk of deadlock. This thread can be disabled
            (making item expiry a no-op) with the this option.

        :param expire_scan_interval: Set the frequency (in seconds) of cache sweeper scans. Default: 3.

        :param report_freed: Constantly report the cache item freed by the sweeper.

            .. warning:: Use only for debug.

        """
        self._set('cache-no-expire', no_expire, cast=bool)
        self._set('cache-report-freed-items', report_freed, cast=bool)
        self._set('cache-expire-freq', expire_scan_interval)

        return self._section

    def add_item(self, key: str, value: str, *, cache_name: str = None):
        """Add an item into the given cache.

        This is a commodity option (mainly useful for testing) allowing you
        to store an item in a uWSGI cache during startup.

        :param key:

        :param value:

        :param cache_name: If not set, default will be used.

        """
        cache_name = cache_name or ''
        value = f'{cache_name} {key}={value}'

        self._set('add-cache-item', value.strip(), multi=True)

        return self._section

    def add_file(self, filepath: Union[str, Path], *, gzip: bool = False, cache_name: str = None):
        """Load a static file in the cache.

        .. note:: Items are stored with the filepath as is (relative or absolute) as the key.

        :param filepath:

        :param gzip: Use gzip compression.

        :param cache_name: If not set, default will be used.

        """
        command = 'load-file-in-cache'

        if gzip:
            command += '-gzip'

        cache_name = cache_name or ''
        value = f'{cache_name} {filepath}'

        self._set(command, value.strip(), multi=True)

        return self._section

    def add_cache(
            self,
            name: str,
            *,
            max_items: int,
            no_expire: bool = None,
            store: str = None,
            store_sync_interval: int = None,
            store_delete: bool = None,
            hash_algo: str = None,
            hash_size: int = None,
            key_size: int = None,
            udp_clients: Strlist = None,
            udp_servers: Strlist = None,
            block_size: int = None,
            block_count: int = None,
            sync_from: Strlist = None,
            mode_bitmap: bool = None,
            use_lastmod: bool = None,
            full_silent: bool = None,
            full_purge_lru: bool = None
    ):
        """Creates cache. Default mode: single block.

        .. note:: This uses new generation ``cache2`` option available since uWSGI 1.9.

        .. note:: When at least one cache is configured without ``full_purge_lru``
            and the master is enabled a thread named "the cache sweeper" is started.
            Its main purpose is deleting expired keys from the cache.
            If you want auto-expiring you need to enable the master.

        :param name: Set the name of the cache. Must be unique in an instance.

        :param max_items: Set the maximum number of cache items.

            .. note:: Effective number of items is **max_items - 1** -
                the first item of the cache is always internally used as "NULL/None/undef".

        :param no_expire: If ``True`` cache items won't expire even if instructed
            to do so by cache set method.

        :param store: Set the filename for the persistent storage.
            If it doesn't exist, the system assumes an empty cache and the file will be created.

        :param store_sync_interval: Set the number of seconds after which msync() is called
            to flush memory cache on disk when in persistent mode.
            By default it is disabled leaving the decision-making to the kernel.

        :param store_delete: uWSGI, by default, will not start if a cache file exists
            and the store file does not match the configured items/blocksize.
            Setting this option will make uWSGI delete the existing file upon mismatch
            and create a new one.

        :param hash_algo: Set the hash algorithm used in the hash table. Current options are:

            * djb33x (default)
            * murmur2

        :param hash_size: This is the size of the hash table in bytes.
            Generally 65536 (the default) is a good value.

            .. note:: Change it only if you know what you are doing
                or if you have a lot of collisions in your cache.

        :param key_size: Set the maximum size of a key, in bytes. Default: 2048.

        :param udp_clients: List of UDP servers which will receive UDP cache updates.

        :param udp_servers: List of UDP addresses on which to bind the cache
            to wait for UDP updates.

        :param block_size: Set the size (in bytes) of a single block.

            .. note:: It's a good idea to use a multiple of 4096 (common memory page size).

        :param block_count: Set the number of blocks in the cache. Useful only in bitmap mode,
            otherwise the number of blocks is equal to the maximum number of items.

        :param sync_from: List of uWSGI addresses which the cache subsystem will connect to
            for getting a full dump of the cache. It can be used for initial cache synchronization.
            The first node sending a valid dump will stop the procedure.
            
        :param mode_bitmap: Enable (more versatile but relatively slower) bitmap mode.

            http://uwsgi-docs.readthedocs.io/en/latest/Caching.html#single-block-faster-vs-bitmaps-slower

            .. warning:: Considered production ready only from uWSGI 2.0.2.

        :param use_lastmod: Enabling will update last_modified_at timestamp of each cache
            on every cache item modification. Enable it if you want to track this value
            or if other features depend on it. This value will then be accessible via the stats socket.

        :param full_silent: By default uWSGI will print warning message on every cache set operation
            if the cache is full. To disable this warning set this option.

            .. note:: Available since 2.0.4.

        :param full_purge_lru: Allows the caching framework to evict Least Recently Used (LRU)
            item when you try to add new item to cache storage that is full.

            .. note:: ``no_expire`` argument will be ignored.

        """
        value = KeyValue(
            locals(),
            keys=[
                'name', 'max_items', 'no_expire', 'store', 'store_sync_interval', 'store_delete',
                'hash_algo', 'hash_size', 'key_size', 'udp_clients', 'udp_servers',
                'block_size', 'block_count', 'sync_from', 'mode_bitmap', 'use_lastmod',
                'full_silent', 'full_purge_lru',
            ],
            aliases={
                'max_items': 'maxitems',
                'store_sync_interval': 'storesync',
                'hash_algo': 'hash',
                'udp_clients': 'nodes',
                'block_size': 'blocksize',
                'block_count': 'blocks',
                'sync_from': 'sync',
                'mode_bitmap': 'bitmap',
                'use_lastmod': 'lastmod',
                'full_silent': 'ignore_full',
                'full_purge_lru': 'purge_lru',
            },
            bool_keys=['store_delete', 'mode_bitmap', 'use_lastmod', 'full_silent', 'full_purge_lru', 'no_expire'],
            list_keys=['udp_clients', 'udp_servers', 'sync_from'],
        )

        self._set('cache2', value, multi=True)

        return self._section
