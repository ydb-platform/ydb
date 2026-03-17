"""Intercept HTTP connections that use
`httplib2 <https://github.com/jcgregorio/httplib2>`_.
"""

import sys

from httplib2 import (SCHEME_TO_CONNECTION, HTTPConnectionWithTimeout,
                      HTTPSConnectionWithTimeout)

from . import (HTTPConnection, HTTPSConnection, WSGI_HTTPConnection,
               WSGI_HTTPSConnection)

HTTPInterceptorMixin = WSGI_HTTPConnection
HTTPSInterceptorMixin = WSGI_HTTPSConnection


class HTTP_WSGIInterceptorWithTimeout(HTTPInterceptorMixin,
        HTTPConnectionWithTimeout):
    def __init__(self, host, port=None, strict=None, timeout=None,
            proxy_info=None, source_address=None):

        # In Python3 strict is deprecated
        if sys.version_info[0] < 3:
            HTTPConnection.__init__(self, host, port=port, strict=strict,
                    timeout=timeout, source_address=source_address)
        else:
            HTTPConnection.__init__(self, host, port=port,
                    timeout=timeout, source_address=source_address)


class HTTPS_WSGIInterceptorWithTimeout(HTTPSInterceptorMixin,
        HTTPSConnectionWithTimeout):
    def __init__(self, host, port=None, strict=None, timeout=None,
            proxy_info=None, ca_certs=None, source_address=None,
            **kwargs):

        # ignore proxy_info and ca_certs
        # In Python3 strict is deprecated
        if sys.version_info[0] < 3:
            HTTPSConnection.__init__(self, host, port=port, strict=strict,
                    timeout=timeout, source_address=source_address)
        else:
            HTTPSConnection.__init__(self, host, port=port,
                    timeout=timeout, source_address=source_address)


def install():
    SCHEME_TO_CONNECTION['http'] = HTTP_WSGIInterceptorWithTimeout
    SCHEME_TO_CONNECTION['https'] = HTTPS_WSGIInterceptorWithTimeout


def uninstall():
    SCHEME_TO_CONNECTION['http'] = HTTPConnectionWithTimeout
    SCHEME_TO_CONNECTION['https'] = HTTPSConnectionWithTimeout
