import os
import pytest
from wsgi_intercept import urllib3_intercept, WSGIAppError
from . import wsgi_app
from .install import installer_class, skipnetwork
import urllib3

HOST = 'some_hopefully_nonexistant_domain'

InstalledApp = installer_class(urllib3_intercept)
http = urllib3.PoolManager()


def test_http():
    with InstalledApp(wsgi_app.simple_app, host=HOST, port=80) as app:
        resp = http.request(
            'GET', 'http://some_hopefully_nonexistant_domain:80/')
        assert resp.data == b'WSGI intercept successful!\n'
        assert app.success()


def test_http_default_port():
    with InstalledApp(wsgi_app.simple_app, host=HOST, port=80) as app:
        resp = http.request('GET', 'http://some_hopefully_nonexistant_domain/')
        assert resp.data == b'WSGI intercept successful!\n'
        assert app.success()


def test_http_other_port():
    with InstalledApp(wsgi_app.simple_app, host=HOST, port=8080) as app:
        resp = http.request(
            'GET', 'http://some_hopefully_nonexistant_domain:8080/')
        assert resp.data == b'WSGI intercept successful!\n'
        assert app.success()
        environ = app.get_internals()
        assert environ['wsgi.url_scheme'] == 'http'


def test_bogus_domain():
    with InstalledApp(wsgi_app.simple_app, host=HOST, port=80):
        with pytest.raises(urllib3.exceptions.ProtocolError):
            http.request("GET", "http://_nonexistant_domain_", retries=False)


def test_proxy_handling():
    with pytest.raises(RuntimeError) as exc:
        with InstalledApp(wsgi_app.simple_app, host=HOST, port=80,
                          proxy='some_proxy.com:1234'):
            http.request('GET', 'http://some_hopefully_nonexistant_domain:80/')
    assert 'http_proxy or https_proxy set in environment' in str(exc.value)
    # We need to do this by hand because the exception was raised
    # during the entry of the context manager, so the exit handler
    # wasn't reached.
    del os.environ['http_proxy']


def test_https():
    with InstalledApp(wsgi_app.simple_app, host=HOST, port=443) as app:
        resp = http.request(
            'GET', 'https://some_hopefully_nonexistant_domain:443/')
        assert resp.data == b'WSGI intercept successful!\n'
        assert app.success()


def test_https_default_port():
    with InstalledApp(wsgi_app.simple_app, host=HOST, port=443) as app:
        resp = http.request(
            'GET', 'https://some_hopefully_nonexistant_domain/')
        assert resp.data == b'WSGI intercept successful!\n'
        assert app.success()
        environ = app.get_internals()
        assert environ['wsgi.url_scheme'] == 'https'


def test_socket_options():
    http = urllib3.PoolManager(socket_options=[])
    with InstalledApp(wsgi_app.simple_app, host=HOST, port=80):
        http.request('GET', 'http://some_hopefully_nonexistant_domain/')
    with InstalledApp(wsgi_app.simple_app, host=HOST, port=443):
        http.request('GET', 'https://some_hopefully_nonexistant_domain/')


def test_app_error():
    with InstalledApp(wsgi_app.raises_app, host=HOST, port=80):
        with pytest.raises(WSGIAppError):
            http.request('GET', 'http://some_hopefully_nonexistant_domain/')


@skipnetwork
def test_http_not_intercepted():
    with InstalledApp(wsgi_app.raises_app, host=HOST, port=80):
        resp = http.request('GET', 'http://google.com')
        assert resp.status >= 200 and resp.status < 300


@skipnetwork
def test_https_not_intercepted():
    with InstalledApp(wsgi_app.raises_app, host=HOST, port=80):
        resp = http.request('GET', 'https://google.com')
        assert resp.status >= 200 and resp.status < 300
