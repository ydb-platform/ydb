"""Common code of urllib3 (<2.0.0) and requests intercepts."""

import os
import sys

from . import WSGI_HTTPConnection, WSGI_HTTPSConnection, wsgi_fake_socket


wsgi_fake_socket.settimeout = lambda self, timeout: None


def make_urllib3_override(HTTPConnectionPool, HTTPSConnectionPool,
                          HTTPConnection, HTTPSConnection):

    class HTTP_WSGIInterceptor(WSGI_HTTPConnection, HTTPConnection):
        def __init__(self, *args, **kwargs):
            if 'strict' in kwargs and sys.version_info > (3, 0):
                kwargs.pop('strict')
            kwargs.pop('socket_options', None)
            kwargs.pop('server_hostname', None)
            WSGI_HTTPConnection.__init__(self, *args, **kwargs)
            HTTPConnection.__init__(self, *args, **kwargs)

    class HTTPS_WSGIInterceptor(WSGI_HTTPSConnection, HTTPSConnection):
        is_verified = True

        def __init__(self, *args, **kwargs):
            if 'strict' in kwargs and sys.version_info > (3, 0):
                kwargs.pop('strict')
            kwargs.pop('socket_options', None)
            kwargs.pop('key_password', None)
            kwargs.pop('server_hostname', None)
            kwargs.pop('ssl_context', None)
            if sys.version_info > (3, 12):
                kwargs.pop('key_file', None)
                kwargs.pop('cert_file', None)
            WSGI_HTTPSConnection.__init__(self, *args, **kwargs)
            HTTPSConnection.__init__(self, *args, **kwargs)

    def install():
        if 'http_proxy' in os.environ or 'https_proxy' in os.environ:
            raise RuntimeError(
                'http_proxy or https_proxy set in environment, please unset')
        HTTPConnectionPool.ConnectionCls = HTTP_WSGIInterceptor
        HTTPSConnectionPool.ConnectionCls = HTTPS_WSGIInterceptor

    def uninstall():
        HTTPConnectionPool.ConnectionCls = HTTPConnection
        HTTPSConnectionPool.ConnectionCls = HTTPSConnection

    return install, uninstall
