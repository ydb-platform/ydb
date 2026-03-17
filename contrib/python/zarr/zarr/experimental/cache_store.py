from __future__ import annotations

import asyncio
import logging
import time
from collections import OrderedDict
from typing import TYPE_CHECKING, Any, Literal

from zarr.abc.store import ByteRequest, Store
from zarr.storage._wrapper import WrapperStore

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from zarr.core.buffer.core import Buffer, BufferPrototype


class CacheStore(WrapperStore[Store]):
    """
    A dual-store caching implementation for Zarr stores.

    This cache wraps any Store implementation and uses a separate Store instance
    as the cache backend. This provides persistent caching capabilities with
    time-based expiration, size-based eviction, and flexible cache storage options.

    Parameters
    ----------
    store : Store
        The underlying store to wrap with caching
    cache_store : Store
        The store to use for caching (can be any Store implementation)
    max_age_seconds : int | None, optional
        Maximum age of cached entries in seconds. None means no expiration.
        Default is None.
    max_size : int | None, optional
        Maximum size of the cache in bytes. When exceeded, least recently used
        items are evicted. None means unlimited size. Default is None.
        Note: Individual values larger than max_size will not be cached.
    key_insert_times : dict[str, float] | None, optional
        Dictionary to track insertion times (using monotonic time).
        Primarily for internal use. Default is None (creates new dict).
    cache_set_data : bool, optional
        Whether to cache data when it's written to the store. Default is True.

    Examples
    --------
    ```python
    import zarr
    from zarr.storage import MemoryStore
    from zarr.experimental.cache_store import CacheStore

    # Create a cached store
    source_store = MemoryStore()
    cache_store = MemoryStore()
    cached_store = CacheStore(
        store=source_store,
        cache_store=cache_store,
        max_age_seconds=60,
        max_size=1024*1024
    )

    # Use it like any other store
    array = zarr.create(shape=(100,), store=cached_store)
    array[:] = 42
    ```

    """

    _cache: Store
    max_age_seconds: int | Literal["infinity"]
    max_size: int | None
    key_insert_times: dict[str, float]
    cache_set_data: bool
    _cache_order: OrderedDict[str, None]  # Track access order for LRU
    _current_size: int  # Track current cache size
    _key_sizes: dict[str, int]  # Track size of each cached key
    _lock: asyncio.Lock
    _hits: int  # Cache hit counter
    _misses: int  # Cache miss counter
    _evictions: int  # Cache eviction counter

    def __init__(
        self,
        store: Store,
        *,
        cache_store: Store,
        max_age_seconds: int | str = "infinity",
        max_size: int | None = None,
        key_insert_times: dict[str, float] | None = None,
        cache_set_data: bool = True,
    ) -> None:
        super().__init__(store)

        if not cache_store.supports_deletes:
            msg = (
                f"The provided cache store {cache_store} does not support deletes. "
                "The cache_store must support deletes for CacheStore to function properly."
            )
            raise ValueError(msg)

        self._cache = cache_store
        # Validate and set max_age_seconds
        if isinstance(max_age_seconds, str):
            if max_age_seconds != "infinity":
                raise ValueError("max_age_seconds string value must be 'infinity'")
            self.max_age_seconds = "infinity"
        else:
            self.max_age_seconds = max_age_seconds
        self.max_size = max_size
        if key_insert_times is None:
            self.key_insert_times = {}
        else:
            self.key_insert_times = key_insert_times
        self.cache_set_data = cache_set_data
        self._cache_order = OrderedDict()
        self._current_size = 0
        self._key_sizes = {}
        self._lock = asyncio.Lock()
        self._hits = 0
        self._misses = 0
        self._evictions = 0

    def _is_key_fresh(self, key: str) -> bool:
        """Check if a cached key is still fresh based on max_age_seconds.

        Uses monotonic time for accurate elapsed time measurement.
        """
        if self.max_age_seconds == "infinity":
            return True
        now = time.monotonic()
        elapsed = now - self.key_insert_times.get(key, 0)
        return elapsed < self.max_age_seconds

    async def _accommodate_value(self, value_size: int) -> None:
        """Ensure there is enough space in the cache for a new value.

        Must be called while holding self._lock.
        """
        if self.max_size is None:
            return

        # Remove least recently used items until we have enough space
        while self._current_size + value_size > self.max_size and self._cache_order:
            # Get the least recently used key (first in OrderedDict)
            lru_key = next(iter(self._cache_order))
            await self._evict_key(lru_key)

    async def _evict_key(self, key: str) -> None:
        """Evict a key from the cache.

        Must be called while holding self._lock.
        Updates size tracking atomically with deletion.
        """
        try:
            key_size = self._key_sizes.get(key, 0)

            # Delete from cache store
            await self._cache.delete(key)

            # Update tracking after successful deletion
            self._remove_from_tracking(key)
            self._current_size = max(0, self._current_size - key_size)
            self._evictions += 1

            logger.debug("_evict_key: evicted key %s, freed %d bytes", key, key_size)
        except Exception:
            logger.exception("_evict_key: failed to evict key %s", key)
            raise  # Re-raise to signal eviction failure

    async def _cache_value(self, key: str, value: Buffer) -> None:
        """Cache a value with size tracking.

        This method holds the lock for the entire operation to ensure atomicity.
        """
        value_size = len(value)

        # Check if value exceeds max size
        if self.max_size is not None and value_size > self.max_size:
            logger.warning(
                "_cache_value: value size %d exceeds max_size %d, skipping cache",
                value_size,
                self.max_size,
            )
            return

        async with self._lock:
            # If key already exists, subtract old size first
            if key in self._key_sizes:
                old_size = self._key_sizes[key]
                self._current_size -= old_size
                logger.debug("_cache_value: updating existing key %s, old size %d", key, old_size)

            # Make room for the new value (this calls _evict_key_locked internally)
            await self._accommodate_value(value_size)

            # Update tracking atomically
            self._cache_order[key] = None  # OrderedDict to track access order
            self._current_size += value_size
            self._key_sizes[key] = value_size
            self.key_insert_times[key] = time.monotonic()

            logger.debug("_cache_value: cached key %s with size %d bytes", key, value_size)

    async def _update_access_order(self, key: str) -> None:
        """Update the access order for LRU tracking."""
        if key in self._cache_order:
            async with self._lock:
                # Move to end (most recently used)
                self._cache_order.move_to_end(key)

    def _remove_from_tracking(self, key: str) -> None:
        """Remove a key from all tracking structures.

        Must be called while holding self._lock.
        """
        self._cache_order.pop(key, None)
        self.key_insert_times.pop(key, None)
        self._key_sizes.pop(key, None)

    async def _get_try_cache(
        self, key: str, prototype: BufferPrototype, byte_range: ByteRequest | None = None
    ) -> Buffer | None:
        """Try to get data from cache first, falling back to source store."""
        maybe_cached_result = await self._cache.get(key, prototype, byte_range)
        if maybe_cached_result is not None:
            logger.debug("_get_try_cache: key %s found in cache (HIT)", key)
            self._hits += 1
            # Update access order for LRU
            await self._update_access_order(key)
            return maybe_cached_result
        else:
            logger.debug(
                "_get_try_cache: key %s not found in cache (MISS), fetching from store", key
            )
            self._misses += 1
            maybe_fresh_result = await super().get(key, prototype, byte_range)
            if maybe_fresh_result is None:
                # Key doesn't exist in source store
                await self._cache.delete(key)
                async with self._lock:
                    self._remove_from_tracking(key)
            else:
                # Cache the newly fetched value
                await self._cache.set(key, maybe_fresh_result)
                await self._cache_value(key, maybe_fresh_result)
            return maybe_fresh_result

    async def _get_no_cache(
        self, key: str, prototype: BufferPrototype, byte_range: ByteRequest | None = None
    ) -> Buffer | None:
        """Get data directly from source store and update cache."""
        self._misses += 1
        maybe_fresh_result = await super().get(key, prototype, byte_range)
        if maybe_fresh_result is None:
            # Key doesn't exist in source, remove from cache and tracking
            await self._cache.delete(key)
            async with self._lock:
                self._remove_from_tracking(key)
        else:
            logger.debug("_get_no_cache: key %s found in store, setting in cache", key)
            await self._cache.set(key, maybe_fresh_result)
            await self._cache_value(key, maybe_fresh_result)
        return maybe_fresh_result

    async def get(
        self,
        key: str,
        prototype: BufferPrototype,
        byte_range: ByteRequest | None = None,
    ) -> Buffer | None:
        """
        Retrieve data from the store, using cache when appropriate.

        Parameters
        ----------
        key : str
            The key to retrieve
        prototype : BufferPrototype
            Buffer prototype for creating the result buffer
        byte_range : ByteRequest, optional
            Byte range to retrieve

        Returns
        -------
        Buffer | None
            The retrieved data, or None if not found
        """
        if not self._is_key_fresh(key):
            logger.debug("get: key %s is not fresh, fetching from store", key)
            return await self._get_no_cache(key, prototype, byte_range)
        else:
            logger.debug("get: key %s is fresh, trying cache", key)
            return await self._get_try_cache(key, prototype, byte_range)

    async def set(self, key: str, value: Buffer) -> None:
        """
        Store data in the underlying store and optionally in cache.

        Parameters
        ----------
        key : str
            The key to store under
        value : Buffer
            The data to store
        """
        logger.debug("set: setting key %s in store", key)
        await super().set(key, value)
        if self.cache_set_data:
            logger.debug("set: setting key %s in cache", key)
            await self._cache.set(key, value)
            await self._cache_value(key, value)
        else:
            logger.debug("set: deleting key %s from cache", key)
            await self._cache.delete(key)
            async with self._lock:
                self._remove_from_tracking(key)

    async def delete(self, key: str) -> None:
        """
        Delete data from both the underlying store and cache.

        Parameters
        ----------
        key : str
            The key to delete
        """
        logger.debug("delete: deleting key %s from store", key)
        await super().delete(key)
        logger.debug("delete: deleting key %s from cache", key)
        await self._cache.delete(key)
        async with self._lock:
            self._remove_from_tracking(key)

    def cache_info(self) -> dict[str, Any]:
        """Return information about the cache state."""
        return {
            "cache_store_type": type(self._cache).__name__,
            "max_age_seconds": "infinity"
            if self.max_age_seconds == "infinity"
            else self.max_age_seconds,
            "max_size": self.max_size,
            "current_size": self._current_size,
            "cache_set_data": self.cache_set_data,
            "tracked_keys": len(self.key_insert_times),
            "cached_keys": len(self._cache_order),
        }

    def cache_stats(self) -> dict[str, Any]:
        """Return cache performance statistics."""
        total_requests = self._hits + self._misses
        hit_rate = self._hits / total_requests if total_requests > 0 else 0.0
        return {
            "hits": self._hits,
            "misses": self._misses,
            "evictions": self._evictions,
            "total_requests": total_requests,
            "hit_rate": hit_rate,
        }

    async def clear_cache(self) -> None:
        """Clear all cached data and tracking information."""
        # Clear the cache store if it supports clear
        if hasattr(self._cache, "clear"):
            await self._cache.clear()

        # Reset tracking
        async with self._lock:
            self.key_insert_times.clear()
            self._cache_order.clear()
            self._key_sizes.clear()
            self._current_size = 0
        logger.debug("clear_cache: cleared all cache data")

    def __repr__(self) -> str:
        """Return string representation of the cache store."""
        return (
            f"{self.__class__.__name__}("
            f"store={self._store!r}, "
            f"cache_store={self._cache!r}, "
            f"max_age_seconds={self.max_age_seconds}, "
            f"max_size={self.max_size}, "
            f"current_size={self._current_size}, "
            f"cached_keys={len(self._cache_order)})"
        )
