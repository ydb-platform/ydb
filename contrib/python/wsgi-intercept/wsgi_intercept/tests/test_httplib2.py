import pytest
from wsgi_intercept import httplib2_intercept, WSGIAppError
from . import wsgi_app
from .install import installer_class
import httplib2
from socket import gaierror

HOST = 'some_hopefully_nonexistant_domain'

InstalledApp = installer_class(httplib2_intercept)


def test_http():
    with InstalledApp(wsgi_app.simple_app, host=HOST, port=80) as app:
        http = httplib2.Http()
        resp, content = http.request(
            'http://some_hopefully_nonexistant_domain:80/')
        assert content == b'WSGI intercept successful!\n'
        assert app.success()


def test_http_default_port():
    with InstalledApp(wsgi_app.simple_app, host=HOST, port=80) as app:
        http = httplib2.Http()
        resp, content = http.request(
            'http://some_hopefully_nonexistant_domain/')
        assert content == b'WSGI intercept successful!\n'
        assert app.success()


def test_http_other_port():
    with InstalledApp(wsgi_app.simple_app, host=HOST, port=8080) as app:
        http = httplib2.Http()
        resp, content = http.request(
            'http://some_hopefully_nonexistant_domain:8080/')
        assert content == b'WSGI intercept successful!\n'
        assert app.success()

        environ = app.get_internals()
        assert environ['wsgi.url_scheme'] == 'http'


def test_bogus_domain():
    with InstalledApp(wsgi_app.simple_app, host=HOST, port=80):
        with pytest.raises(gaierror):
            httplib2_intercept.HTTP_WSGIInterceptorWithTimeout(
                    "_nonexistant_domain_").connect()


def test_proxy_handling():
    """Proxy has no impact."""
    with InstalledApp(wsgi_app.simple_app, host=HOST, port=80,
                      proxy='some_proxy.com:1234') as app:
        http = httplib2.Http()
        resp, content = http.request(
            'http://some_hopefully_nonexistant_domain:80/')
        assert content == b'WSGI intercept successful!\n'
        assert app.success()


def test_https():
    with InstalledApp(wsgi_app.simple_app, host=HOST, port=443) as app:
        http = httplib2.Http()
        resp, content = http.request(
            'https://some_hopefully_nonexistant_domain:443/')
        assert app.success()


def test_https_default_port():
    with InstalledApp(wsgi_app.simple_app, host=HOST, port=443) as app:
        http = httplib2.Http()
        resp, content = http.request(
            'https://some_hopefully_nonexistant_domain/')
        assert app.success()

        environ = app.get_internals()
        assert environ['wsgi.url_scheme'] == 'https'


def test_app_error():
    with InstalledApp(wsgi_app.raises_app, host=HOST, port=80):
        http = httplib2.Http()
        with pytest.raises(WSGIAppError):
            http.request(
                'http://some_hopefully_nonexistant_domain/')
