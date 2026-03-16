import datetime

import freezegun

from zeep import cache


def test_sqlite_cache(tmpdir):
    c = cache.SqliteCache(path=tmpdir.join("sqlite.cache.db").strpath)
    c.add("http://tests.python-zeep.org/example.wsdl", b"content")

    result = c.get("http://tests.python-zeep.org/example.wsdl")
    assert result == b"content"


def test_sqlite_cache_timeout(tmpdir):
    c = cache.SqliteCache(path=tmpdir.join("sqlite.cache.db").strpath)
    c.add("http://tests.python-zeep.org/example.wsdl", b"content")
    result = c.get("http://tests.python-zeep.org/example.wsdl")
    assert result == b"content"

    freeze_dt = datetime.datetime.utcnow() + datetime.timedelta(seconds=7200)
    with freezegun.freeze_time(freeze_dt):
        result = c.get("http://tests.python-zeep.org/example.wsdl")
        assert result is None


def test_memory_cache_timeout(tmpdir):
    c = cache.InMemoryCache()
    c.add("http://tests.python-zeep.org/example.wsdl", b"content")
    result = c.get("http://tests.python-zeep.org/example.wsdl")
    assert result == b"content"

    freeze_dt = datetime.datetime.utcnow() + datetime.timedelta(seconds=7200)
    with freezegun.freeze_time(freeze_dt):
        result = c.get("http://tests.python-zeep.org/example.wsdl")
        assert result is None


def test_memory_cache_share_data(tmpdir):
    a = cache.InMemoryCache()
    b = cache.InMemoryCache()
    a.add("http://tests.python-zeep.org/example.wsdl", b"content")

    result = b.get("http://tests.python-zeep.org/example.wsdl")
    assert result == b"content"
