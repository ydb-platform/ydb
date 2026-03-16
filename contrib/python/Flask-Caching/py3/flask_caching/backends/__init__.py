"""
    flask_caching.backends
    ~~~~~~~~~~~~~~~~~~~~~~

    Various caching backends.

    :copyright: (c) 2018 by Peter Justin.
    :copyright: (c) 2010 by Thadeus Burgess.
    :license: BSD, see LICENSE for more details.
"""

from flask_caching.backends.filesystemcache import FileSystemCache
from flask_caching.backends.memcache import MemcachedCache
from flask_caching.backends.memcache import SASLMemcachedCache
from flask_caching.backends.memcache import SpreadSASLMemcachedCache
from flask_caching.backends.nullcache import NullCache
from flask_caching.backends.rediscache import RedisCache
from flask_caching.backends.rediscache import RedisClusterCache
from flask_caching.backends.rediscache import RedisSentinelCache
from flask_caching.backends.simplecache import SimpleCache
from flask_caching.backends.uwsgicache import UWSGICache


__all__ = (
    "null",
    "simple",
    "filesystem",
    "redis",
    "redissentinel",
    "rediscluster",
    "uwsgi",
    "memcached",
    "gaememcached",
    "saslmemcached",
    "spreadsaslmemcached",
)


def null(app, config, args, kwargs):
    return NullCache.factory(app, config, args, kwargs)


def simple(app, config, args, kwargs):
    return SimpleCache.factory(app, config, args, kwargs)


def filesystem(app, config, args, kwargs):
    return FileSystemCache.factory(app, config, args, kwargs)


def redis(app, config, args, kwargs):
    return RedisCache.factory(app, config, args, kwargs)


def redissentinel(app, config, args, kwargs):
    return RedisSentinelCache.factory(app, config, args, kwargs)


def rediscluster(app, config, args, kwargs):
    return RedisClusterCache.factory(app, config, args, kwargs)


def uwsgi(app, config, args, kwargs):
    return UWSGICache.factory(app, config, args, kwargs)


def memcached(app, config, args, kwargs):
    return MemcachedCache.factory(app, config, args, kwargs)


def gaememcached(app, config, args, kwargs):
    return memcached(app, config, args, kwargs)


def saslmemcached(app, config, args, kwargs):
    return SASLMemcachedCache.factory(app, config, args, kwargs)


def spreadsaslmemcached(app, config, args, kwargs):
    return SpreadSASLMemcachedCache.factory(app, config, args, kwargs)
