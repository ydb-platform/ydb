import sys

import pytest
import requests

from pytest_localserver import https, plugin


# define test fixture here again in order to run tests without having to
# install the plugin anew every single time
httpsserver = plugin.httpsserver


def test_httpsserver_funcarg(httpsserver):
    assert isinstance(httpsserver, https.SecureContentServer)
    assert httpsserver.is_alive()
    assert httpsserver.server_address


def test_server_does_not_serve_file_at_startup(httpsserver):
    assert httpsserver.code == 204
    assert httpsserver.content == ''


def test_some_content_retrieval(httpsserver):
    httpsserver.serve_content('TEST!')
    resp = requests.get(httpsserver.url, verify=False)
    assert resp.text == 'TEST!'
    assert resp.status_code == 200


def test_GET_request(httpsserver):
    httpsserver.serve_content('TEST!', headers={'Content-type': 'text/plain'})
    resp = requests.get(httpsserver.url, headers={'User-Agent': 'Test method'}, verify=False)
    assert resp.text == 'TEST!'
    assert resp.status_code == 200
    assert 'text/plain' in resp.headers['Content-type']


def test_HEAD_request(httpsserver):
    httpsserver.serve_content('TEST!', headers={'Content-type': 'text/plain'})
    print(httpsserver.url)
    resp = requests.head(httpsserver.url, verify=False)
    assert resp.status_code == 200
    assert resp.headers['Content-type'] == 'text/plain'
