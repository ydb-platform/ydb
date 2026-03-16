"""Intercept HTTP connections that use httplib (Py2) or http.client (Py3).
"""

try:
    import http.client as http_lib
except ImportError:
    import httplib as http_lib

from . import WSGI_HTTPConnection, WSGI_HTTPSConnection

try:
    from http.client import (
            HTTPConnection as OriginalHTTPConnection,
            HTTPSConnection as OriginalHTTPSConnection
    )
except ImportError:
    from httplib import (
            HTTPConnection as OriginalHTTPConnection,
            HTTPSConnection as OriginalHTTPSConnection
    )


class HTTP_WSGIInterceptor(WSGI_HTTPConnection, http_lib.HTTPConnection):
    pass


class HTTPS_WSGIInterceptor(WSGI_HTTPSConnection, http_lib.HTTPSConnection,
                            HTTP_WSGIInterceptor):

    def __init__(self, host, **kwargs):
        self.host = host
        try:
            self.port = kwargs['port']
        except KeyError:
            self.port = None
        HTTP_WSGIInterceptor.__init__(self, host, **kwargs)


def install():
    http_lib.HTTPConnection = HTTP_WSGIInterceptor
    http_lib.HTTPSConnection = HTTPS_WSGIInterceptor


def uninstall():
    http_lib.HTTPConnection = OriginalHTTPConnection
    http_lib.HTTPSConnection = OriginalHTTPSConnection
