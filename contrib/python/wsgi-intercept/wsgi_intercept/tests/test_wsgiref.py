import requests
import wsgiref.simple_server

from wsgi_intercept.interceptor import RequestsInterceptor


def load_app():
    return wsgiref.simple_server.demo_app


def test_wsgiref():
    """General test that the wsgiref server behaves.

    This mostly confirms that environ handling is correct in both
    python2 and 3.
    """

    try:
        with RequestsInterceptor(load_app, host='www.example.net', port=80):
            r = requests.get('http://www.example.net')
            print(r.text)
    except Exception as exc:
        assert False, 'wsgi ref server raised exception: %s' % exc
