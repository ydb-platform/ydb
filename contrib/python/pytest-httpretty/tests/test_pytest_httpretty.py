import sys

import httpretty
import pytest
import requests

from pytest_httpretty import stub_get


@pytest.fixture
def dummy():
    return


@httpretty.activate
def test_httpretty_activate():
    httpretty.register_uri(httpretty.GET, 'http://example.com/', body='Hello')

    assert requests.get('http://example.com').text == 'Hello'


@pytest.mark.xfail(sys.version_info[0] == 2,
                   reason=('httpretty.activate() does not work with pytest '
                           'fixtures in Python 2'))
@httpretty.activate
def test_httpretty_activate_with_pytest_fixtures(dummy):
    httpretty.register_uri(httpretty.GET, 'http://example.com/', body='Hello')

    assert requests.get('http://example.com').text == 'Hello'


@pytest.mark.httpretty
def test_mark_httpretty(dummy):
    httpretty.register_uri(httpretty.GET, 'http://example.com/', body='Hello')

    assert requests.get('http://example.com').text == 'Hello'


@pytest.mark.httpretty
def test_stub_get(dummy):
    stub_get('http://example.com/', body='World!')

    assert requests.get('http://example.com').text == 'World!'
