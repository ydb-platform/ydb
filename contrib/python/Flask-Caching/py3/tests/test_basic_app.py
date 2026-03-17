import pytest
from flask import Flask

from flask_caching import Cache
from flask_caching.backends.simplecache import SimpleCache

try:
    import redis  # noqa

    HAS_NOT_REDIS = False
except ImportError:
    HAS_NOT_REDIS = True


class CustomCache(Cache):
    pass


class CustomSimpleCache(SimpleCache):
    pass


def newsimple(app, config, args, kwargs):
    return CustomSimpleCache(*args, **kwargs)


def test_dict_config(app):
    cache = Cache(config={"CACHE_TYPE": "simple"})
    cache.init_app(app)

    assert cache.config["CACHE_TYPE"] == "simple"


def test_dict_config_initapp(app):
    cache = Cache()
    cache.init_app(app, config={"CACHE_TYPE": "simple"})
    from flask_caching.backends.simplecache import SimpleCache

    assert isinstance(app.extensions["cache"][cache], SimpleCache)


def test_dict_config_both(app):
    cache = Cache(config={"CACHE_TYPE": "null"})
    cache.init_app(app, config={"CACHE_TYPE": "simple"})
    from flask_caching.backends.simplecache import SimpleCache

    assert isinstance(app.extensions["cache"][cache], SimpleCache)


def test_init_app_sets_app_attribute(app):
    cache = Cache()
    cache.init_app(app)
    assert cache.app == app


@pytest.mark.skipif(HAS_NOT_REDIS, reason="requires Redis")
def test_init_app_multi_apps(app, redis_server):
    cache = Cache()
    app1 = Flask(__name__)
    app1.config.from_mapping({"CACHE_TYPE": "redis", "CACHE_KEY_PREFIX": "foo"})

    app2 = Flask(__name__)
    app2.config.from_mapping({"CACHE_TYPE": "redis", "CACHE_KEY_PREFIX": "bar"})
    cache.init_app(app1)
    cache.init_app(app2)

    # When we have the app context, the prefix should be
    # different for each app.
    with app1.app_context():
        assert cache.cache.key_prefix == "foo"

    with app2.app_context():
        assert cache.cache.key_prefix == "bar"


@pytest.mark.skipif(HAS_NOT_REDIS, reason="requires Redis")
def test_app_redis_cache_backend_url_default_db(app, redis_server):
    config = {
        "CACHE_TYPE": "redis",
        "CACHE_REDIS_URL": "redis://localhost:6379",
    }
    cache = Cache()
    cache.init_app(app, config=config)
    from flask_caching.backends.rediscache import RedisCache

    assert isinstance(app.extensions["cache"][cache], RedisCache)
    rconn = app.extensions["cache"][cache]._write_client.connection_pool.get_connection(
        "foo"
    )
    assert rconn.db == 0


@pytest.mark.skipif(HAS_NOT_REDIS, reason="requires Redis")
def test_app_redis_cache_backend_url_custom_db(app, redis_server):
    config = {
        "CACHE_TYPE": "redis",
        "CACHE_REDIS_URL": "redis://localhost:6379/2",
    }
    cache = Cache()
    cache.init_app(app, config=config)
    rconn = app.extensions["cache"][cache]._write_client.connection_pool.get_connection(
        "foo"
    )
    assert rconn.db == 2


@pytest.mark.skipif(HAS_NOT_REDIS, reason="requires Redis")
def test_app_redis_cache_backend_url_explicit_db_arg(app, redis_server):
    config = {
        "CACHE_TYPE": "redis",
        "CACHE_REDIS_URL": "redis://localhost:6379",
        "CACHE_REDIS_DB": 1,
    }
    cache = Cache()
    cache.init_app(app, config=config)
    rconn = app.extensions["cache"][cache]._write_client.connection_pool.get_connection(
        "foo"
    )
    assert rconn.db == 1


def test_app_custom_cache_backend(app):
    cache = Cache()
    app.config["CACHE_TYPE"] = "__tests__.test_basic_app.newsimple"
    cache.init_app(app)

    with app.app_context():
        assert isinstance(cache.cache, CustomSimpleCache)


def test_subclassed_cache_class(app):
    # just invoking it here proofs that everything worked when subclassing
    # otherwise an werkzeug.utils.ImportStringError exception will be raised
    # because flask-caching can't find the backend

    # testing for "not raises" looked more hacky like this..
    CustomCache(app)
