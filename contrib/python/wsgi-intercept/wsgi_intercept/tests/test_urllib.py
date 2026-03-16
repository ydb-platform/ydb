import os
import pytest
from wsgi_intercept import urllib_intercept, WSGIAppError
from . import wsgi_app
from .install import installer_class, skipnetwork
try:
    import urllib.request as url_lib
except ImportError:
    import urllib2 as url_lib

HOST = 'some_hopefully_nonexistant_domain'

InstalledApp = installer_class(install=urllib_intercept.install_opener)


def test_http():
    with InstalledApp(wsgi_app.simple_app, host=HOST, port=80) as app:
        url_lib.urlopen('http://some_hopefully_nonexistant_domain:80/')
        assert app.success()


def test_http_default_port():
    with InstalledApp(wsgi_app.simple_app, host=HOST, port=80) as app:
        url_lib.urlopen('http://some_hopefully_nonexistant_domain/')
        assert app.success()


def test_http_other_port():
    with InstalledApp(wsgi_app.simple_app, host=HOST, port=8080) as app:
        url_lib.urlopen('http://some_hopefully_nonexistant_domain:8080/')
        assert app.success()
        environ = app.get_internals()
        assert environ['wsgi.url_scheme'] == 'http'


def test_proxy_handling():
    """Like requests, urllib gets confused about proxy early on."""
    with pytest.raises(RuntimeError) as exc:
        with InstalledApp(wsgi_app.simple_app, host=HOST, port=80,
                          proxy='some.host:1234'):
            url_lib.urlopen('http://some_hopefully_nonexistant_domain:80/')
    assert 'http_proxy or https_proxy set in environment' in str(exc.value)
    # We need to do this by hand because the exception was raised
    # during the entry of the context manager, so the exit handler
    # wasn't reached.
    del os.environ['http_proxy']


def test_https():
    with InstalledApp(wsgi_app.simple_app, host=HOST, port=443) as app:
        url_lib.urlopen('https://some_hopefully_nonexistant_domain:443/')
        assert app.success()


def test_https_default_port():
    with InstalledApp(wsgi_app.simple_app, host=HOST, port=443) as app:
        url_lib.urlopen('https://some_hopefully_nonexistant_domain/')
        assert app.success()
        environ = app.get_internals()
        assert environ['wsgi.url_scheme'] == 'https'


def test_app_error():
    with InstalledApp(wsgi_app.raises_app, host=HOST, port=80):
        with pytest.raises(WSGIAppError):
            url_lib.urlopen('http://some_hopefully_nonexistant_domain/')


@skipnetwork
def test_http_not_intercepted():
    with InstalledApp(wsgi_app.simple_app, host=HOST, port=80):
        response = url_lib.urlopen('http://google.com/')
        assert 200 <= int(response.code) < 400


@skipnetwork
def test_https_not_intercepted():
    with InstalledApp(wsgi_app.simple_app, host=HOST, port=443):
        response = url_lib.urlopen('https://google.com/')
        assert 200 <= int(response.code) < 400
