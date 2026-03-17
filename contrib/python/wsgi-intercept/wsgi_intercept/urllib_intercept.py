"""Intercept HTTP connections that use urllib.request (Python 3)
aka urllib2 (Python 2).
"""

import os

try:
    import urllib.request as url_lib
except ImportError:
    import urllib2 as url_lib

from . import WSGI_HTTPConnection, WSGI_HTTPSConnection


class WSGI_HTTPHandler(url_lib.HTTPHandler):
    """
    Override the default HTTPHandler class with one that uses the
    WSGI_HTTPConnection class to open HTTP URLs.
    """
    def http_open(self, req):
        return self.do_open(WSGI_HTTPConnection, req)


class WSGI_HTTPSHandler(url_lib.HTTPSHandler):
    """
    Override the default HTTPSHandler class with one that uses the
    WSGI_HTTPConnection class to open HTTPS URLs.
    """
    def https_open(self, req):
        return self.do_open(WSGI_HTTPSConnection, req)


def install_opener():
    if 'http_proxy' in os.environ or 'https_proxy' in os.environ:
        raise RuntimeError(
            'http_proxy or https_proxy set in environment, please unset')
    handlers = [WSGI_HTTPHandler()]
    if WSGI_HTTPSHandler is not None:
        handlers.append(WSGI_HTTPSHandler())
    opener = url_lib.build_opener(*handlers)
    url_lib.install_opener(opener)

    return opener


def uninstall_opener():
    url_lib.install_opener(None)


install = install_opener
uninstall = uninstall_opener
