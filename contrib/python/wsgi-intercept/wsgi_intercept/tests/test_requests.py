import os
import pytest
from wsgi_intercept import requests_intercept, WSGIAppError
from . import wsgi_app
from .install import installer_class, skipnetwork
import requests
from requests.exceptions import ConnectionError

HOST = 'some_hopefully_nonexistant_domain'

InstalledApp = installer_class(requests_intercept)


def test_http():
    with InstalledApp(wsgi_app.simple_app, host=HOST, port=80) as app:
        resp = requests.get('http://some_hopefully_nonexistant_domain:80/')
        assert resp.content == b'WSGI intercept successful!\n'
        assert app.success()


def test_http_default_port():
    with InstalledApp(wsgi_app.simple_app, host=HOST, port=80) as app:
        resp = requests.get('http://some_hopefully_nonexistant_domain/')
        assert resp.content == b'WSGI intercept successful!\n'
        assert app.success()


def test_http_other_port():
    with InstalledApp(wsgi_app.simple_app, host=HOST, port=8080) as app:
        resp = requests.get('http://some_hopefully_nonexistant_domain:8080/')
        assert resp.content == b'WSGI intercept successful!\n'
        assert app.success()
        environ = app.get_internals()
        assert environ['wsgi.url_scheme'] == 'http'


def test_bogus_domain():
    with InstalledApp(wsgi_app.simple_app, host=HOST, port=80):
        with pytest.raises(ConnectionError):
            requests.get("http://_nonexistant_domain_")


def test_proxy_handling():
    with pytest.raises(RuntimeError) as exc:
        with InstalledApp(wsgi_app.simple_app, host=HOST, port=80,
                          proxy='some_proxy.com:1234'):
            requests.get('http://some_hopefully_nonexistant_domain:80/')
    assert 'http_proxy or https_proxy set in environment' in str(exc.value)
    # We need to do this by hand because the exception was raised
    # during the entry of the context manager, so the exit handler
    # wasn't reached.
    del os.environ['http_proxy']


def test_https():
    with InstalledApp(wsgi_app.simple_app, host=HOST, port=443) as app:
        resp = requests.get('https://some_hopefully_nonexistant_domain:443/')
        assert resp.content == b'WSGI intercept successful!\n'
        assert app.success()


def test_https_no_ssl_verification_intercepted():
    with InstalledApp(wsgi_app.simple_app, host=HOST, port=443) as app:
        resp = requests.get('https://some_hopefully_nonexistant_domain:443/',
                            verify=False)
        assert resp.content == b'WSGI intercept successful!\n'
        assert app.success()


@skipnetwork
def test_https_no_ssl_verification_not_intercepted():
    with InstalledApp(wsgi_app.simple_app, host=HOST, port=443) as app:
        resp = requests.get('https://self-signed.badssl.com/', verify=False)
        assert resp.status_code >= 200 and resp.status_code < 300
        assert not app.success()


def test_https_default_port():
    with InstalledApp(wsgi_app.simple_app, host=HOST, port=443) as app:
        resp = requests.get('https://some_hopefully_nonexistant_domain/')
        assert resp.content == b'WSGI intercept successful!\n'
        assert app.success()
        environ = app.get_internals()
        assert environ['wsgi.url_scheme'] == 'https'


def test_app_error():
    with InstalledApp(wsgi_app.raises_app, host=HOST, port=80):
        with pytest.raises(WSGIAppError):
            requests.get('http://some_hopefully_nonexistant_domain/')


@skipnetwork
def test_http_not_intercepted():
    with InstalledApp(wsgi_app.raises_app, host=HOST, port=80):
        resp = requests.get("http://google.com")
        assert resp.status_code >= 200 and resp.status_code < 300


@skipnetwork
def test_https_not_intercepted():
    with InstalledApp(wsgi_app.raises_app, host=HOST, port=80):
        resp = requests.get("https://google.com")
        assert resp.status_code >= 200 and resp.status_code < 300
