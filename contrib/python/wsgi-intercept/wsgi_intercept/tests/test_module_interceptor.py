"""Test intercepting a full module with interceptor."""

from uuid import uuid4

from httplib2 import Http

from wsgi_intercept.interceptor import Httplib2Interceptor
from .wsgi_app import simple_app


def app():
    return simple_app


def setup_module(module):
    module.host = str(uuid4())
    module.intercept = Httplib2Interceptor(app, host=module.host)
    module.intercept.install_intercept()


def teardown_module(module):
    module.intercept.uninstall_intercept()


def test_simple_request():
    global host
    http = Http()
    response, content = http.request('http://%s/' % host)
    assert response.status == 200
    assert 'WSGI intercept successful!' in content.decode('utf-8')


def test_another_request():
    global host
    http = Http()
    response, content = http.request('http://%s/foobar' % host)
    assert response.status == 200
    assert 'WSGI intercept successful!' in content.decode('utf-8')
