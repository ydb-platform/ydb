# -*- coding: utf-8 -*-
"""
    flask_caching.backends
    ~~~~~~~~~~~~~~~~~~~~~~

    Various caching backends.

    :copyright: (c) 2018 by Peter Justin.
    :copyright: (c) 2010 by Thadeus Burgess.
    :license: BSD, see LICENSE for more details.
"""
from flask_caching.backends.filesystem import FileSystemCache
from flask_caching.backends.memcache import (MemcachedCache,
                                             SASLMemcachedCache,
                                             SpreadSASLMemcachedCache)
from flask_caching.backends.null import NullCache
# TODO: Rename to "redis" when python2 support is removed
from flask_caching.backends.rediscache import RedisCache, RedisSentinelCache
from flask_caching.backends.simple import SimpleCache

try:
    from flask_caching.backends.uwsgicache import UWSGICache

    has_UWSGICache = True
except ImportError:
    has_UWSGICache = False


__all__ = (
    "null",
    "simple",
    "filesystem",
    "redis",
    "redissentinel",
    "uwsgi",
    "memcached",
    "gaememcached",
    "saslmemcached",
    "spreadsaslmemcached",
)


def null(app, config, args, kwargs):
    return NullCache()


def simple(app, config, args, kwargs):
    kwargs.update(
        dict(
            threshold=config["CACHE_THRESHOLD"],
            ignore_errors=config["CACHE_IGNORE_ERRORS"],
        )
    )
    return SimpleCache(*args, **kwargs)


def filesystem(app, config, args, kwargs):
    args.insert(0, config["CACHE_DIR"])
    kwargs.update(
        dict(
            threshold=config["CACHE_THRESHOLD"],
            ignore_errors=config["CACHE_IGNORE_ERRORS"],
        )
    )
    return FileSystemCache(*args, **kwargs)


def redis(app, config, args, kwargs):
    try:
        from redis import from_url as redis_from_url
    except ImportError:
        raise RuntimeError("no redis module found")

    kwargs.update(
        dict(
            host=config.get("CACHE_REDIS_HOST", "localhost"),
            port=config.get("CACHE_REDIS_PORT", 6379),
        )
    )
    password = config.get("CACHE_REDIS_PASSWORD")
    if password:
        kwargs["password"] = password

    key_prefix = config.get("CACHE_KEY_PREFIX")
    if key_prefix:
        kwargs["key_prefix"] = key_prefix

    db_number = config.get("CACHE_REDIS_DB")
    if db_number:
        kwargs["db"] = db_number

    redis_url = config.get("CACHE_REDIS_URL")
    if redis_url:
        kwargs["host"] = redis_from_url(redis_url, db=kwargs.pop("db", None))

    return RedisCache(*args, **kwargs)


def redissentinel(app, config, args, kwargs):
    kwargs.update(
        dict(
            sentinels=config.get(
                "CACHE_REDIS_SENTINELS", [("127.0.0.1", 26379)]
            ),
            master=config.get("CACHE_REDIS_SENTINEL_MASTER", "mymaster"),
            password=config.get("CACHE_REDIS_PASSWORD", None),
            sentinel_password=config.get("CACHE_REDIS_SENTINEL_PASSWORD", None),
            key_prefix=config.get("CACHE_KEY_PREFIX", None),
            db=config.get("CACHE_REDIS_DB", 0),
        )
    )

    return RedisSentinelCache(*args, **kwargs)


def uwsgi(app, config, args, kwargs):
    if not has_UWSGICache:
        raise NotImplementedError(
            "UWSGICache backend is not available, "
            "you should upgrade werkzeug module."
        )
    # The name of the caching instance to connect to, for
    # example: mycache@localhost:3031, defaults to an empty string, which
    # means uWSGI will cache in the local instance. If the cache is in the
    # same instance as the werkzeug app, you only have to provide the name of
    # the cache.
    uwsgi_cache_name = config.get("CACHE_UWSGI_NAME", "")
    kwargs.update(dict(cache=uwsgi_cache_name))
    return UWSGICache(*args, **kwargs)


def memcached(app, config, args, kwargs):
    args.append(config["CACHE_MEMCACHED_SERVERS"])
    kwargs.update(dict(key_prefix=config["CACHE_KEY_PREFIX"]))
    return MemcachedCache(*args, **kwargs)


def gaememcached(app, config, args, kwargs):
    return memcached(app, config, args, kwargs)


def saslmemcached(app, config, args, kwargs):
    args.append(config["CACHE_MEMCACHED_SERVERS"])
    kwargs.update(
        dict(
            username=config["CACHE_MEMCACHED_USERNAME"],
            password=config["CACHE_MEMCACHED_PASSWORD"],
            key_prefix=config["CACHE_KEY_PREFIX"],
        )
    )
    return SASLMemcachedCache(*args, **kwargs)


def spreadsaslmemcached(app, config, args, kwargs):
    args.append(config["CACHE_MEMCACHED_SERVERS"])
    kwargs.update(
        dict(
            username=config.get("CACHE_MEMCACHED_USERNAME"),
            password=config.get("CACHE_MEMCACHED_PASSWORD"),
            key_prefix=config.get("CACHE_KEY_PREFIX"),
        )
    )

    return SpreadSASLMemcachedCache(*args, **kwargs)
