import pytest
from wsgi_intercept import http_client_intercept, WSGIAppError
from . import wsgi_app
from .install import installer_class, skipnetwork
try:
    import http.client as http_lib
except ImportError:
    import httplib as http_lib

HOST = 'some_hopefully_nonexistant_domain'

InstalledApp = installer_class(http_client_intercept)


def test_http():
    with InstalledApp(wsgi_app.simple_app, host=HOST, port=80) as app:
        http_client = http_lib.HTTPConnection(HOST)
        http_client.request('GET', '/')
        content = http_client.getresponse().read()
        http_client.close()
        assert content == b'WSGI intercept successful!\n'
        assert app.success()


def test_https():
    with InstalledApp(wsgi_app.simple_app, host=HOST, port=443) as app:
        http_client = http_lib.HTTPSConnection(HOST)
        http_client.request('GET', '/')
        content = http_client.getresponse().read()
        http_client.close()
        assert content == b'WSGI intercept successful!\n'
        assert app.success()


def test_other():
    with InstalledApp(wsgi_app.simple_app, host=HOST, port=8080) as app:
        http_client = http_lib.HTTPConnection(HOST + ':8080')
        http_client.request('GET', '/')
        content = http_client.getresponse().read()
        http_client.close()
        assert content == b'WSGI intercept successful!\n'
        assert app.success()


def test_proxy_handling():
    """Proxy variable no impact."""
    with InstalledApp(wsgi_app.simple_app, host=HOST, port=80,
                      proxy='some.host:1234') as app:
        http_client = http_lib.HTTPConnection(HOST)
        http_client.request('GET', '/')
        content = http_client.getresponse().read()
        http_client.close()
        assert content == b'WSGI intercept successful!\n'
        assert app.success()


def test_app_error():
    with InstalledApp(wsgi_app.raises_app, host=HOST, port=80):
        http_client = http_lib.HTTPConnection(HOST)
        with pytest.raises(WSGIAppError):
            http_client.request('GET', '/')
            http_client.getresponse().read()
        http_client.close()


@skipnetwork
def test_http_not_intercepted():
    with InstalledApp(wsgi_app.raises_app, host=HOST, port=80):
        http_client = http_lib.HTTPConnection('google.com')
        http_client.request('GET', '/')
        response = http_client.getresponse()
        http_client.close()
        assert 200 <= int(response.status) < 400


@skipnetwork
def test_https_not_intercepted():
    with InstalledApp(wsgi_app.raises_app, host=HOST, port=443):
        http_client = http_lib.HTTPSConnection('google.com')
        http_client.request('GET', '/')
        response = http_client.getresponse()
        http_client.close()
        assert 200 <= int(response.status) < 400
