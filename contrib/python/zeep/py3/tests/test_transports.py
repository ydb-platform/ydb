import os
from unittest.mock import mock_open, patch

import pytest
import requests_mock
from pretend import stub

from zeep import cache, transports


@pytest.mark.requests
def test_no_cache():
    transport = transports.Transport(cache=None)
    assert transport.cache is None


@pytest.mark.requests
def test_custom_cache(tmpdir):
    transport = transports.Transport(cache=cache.SqliteCache(path=tmpdir.join('sqlite.cache.db').strpath))
    assert isinstance(transport.cache, cache.SqliteCache)


def test_load():
    cache = stub(get=lambda url: None, add=lambda url, content: None)
    transport = transports.Transport(cache=cache)

    with requests_mock.mock() as m:
        m.get("http://tests.python-zeep.org/test.xml", text="x")
        result = transport.load("http://tests.python-zeep.org/test.xml")

        assert result == b"x"


@pytest.mark.skipif(os.name != "nt", reason="test valid for windows platform only")
def test_load_file_windows():
    cache = stub(get=lambda url: None, add=lambda url, content: None)
    transport = transports.Transport(cache=cache)
    with patch("io.open", mock_open(read_data=b"x")) as m_open:
        result = transport.load("file://localhost/c:/local/example/example.wsdl")
        assert result == b"x"
        m_open.assert_called_once_with("c:\\local\\example\\example.wsdl", "rb")


@pytest.mark.skipif(os.name == "nt", reason="test valid for unix platform only")
def test_load_file_unix():
    cache = stub(get=lambda url: None, add=lambda url, content: None)
    transport = transports.Transport(cache=cache)
    with patch("io.open", mock_open(read_data=b"x")) as m_open:
        result = transport.load("file:///usr/local/bin/example.wsdl")
        assert result == b"x"
        m_open.assert_called_once_with("/usr/local/bin/example.wsdl", "rb")
        m_open.return_value.close.assert_called()


@pytest.mark.skipif(os.name != "nt", reason="test valid for windows platform only")
def test_load_file_local():
    cache = stub(get=lambda url: None, add=lambda url, content: None)
    transport = transports.Transport(cache=cache)
    with patch("io.open", mock_open(read_data=b"x")) as m_open:
        result = transport.load("file:///c:/local/example/example.wsdl")
        assert result == b"x"
        m_open.assert_called_once_with("c:\\local\\example\\example.wsdl", "rb")


def test_settings_set_context_timeout():
    transport = transports.Transport(cache=cache)

    assert transport.operation_timeout is None
    with transport.settings(timeout=120):
        assert transport.operation_timeout == 120

        with transport.settings(timeout=90):
            assert transport.operation_timeout == 90
        assert transport.operation_timeout == 120
    assert transport.operation_timeout is None
