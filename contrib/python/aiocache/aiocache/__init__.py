import logging
from typing import Dict, Type

from .backends.memory import SimpleMemoryCache
from .base import BaseCache

__version__ = "0.12.3"

logger = logging.getLogger(__name__)

AIOCACHE_CACHES: Dict[str, Type[BaseCache]] = {SimpleMemoryCache.NAME: SimpleMemoryCache}

try:
    import redis
except ImportError:
    logger.debug("redis not installed, RedisCache unavailable")
else:
    from aiocache.backends.redis import RedisCache

    AIOCACHE_CACHES[RedisCache.NAME] = RedisCache
    del redis

try:
    import aiomcache
except ImportError:
    logger.debug("aiomcache not installed, Memcached unavailable")
else:
    from aiocache.backends.memcached import MemcachedCache

    AIOCACHE_CACHES[MemcachedCache.NAME] = MemcachedCache
    del aiomcache

from .decorators import cached, cached_stampede, multi_cached  # noqa: E402,I202
from .factory import Cache, caches  # noqa: E402


__all__ = (
    "caches",
    "Cache",
    "cached",
    "cached_stampede",
    "multi_cached",
    *(c.__name__ for c in AIOCACHE_CACHES.values()),
)
