"""Context manager based WSGI interception.
"""

from importlib import import_module
from uuid import uuid4

from six.moves.urllib import parse as urlparse

import wsgi_intercept


class Interceptor(object):
    """A convenience class over the guts of wsgi_intercept.

    An Interceptor subclass provides a clean entry point to the wsgi_intercept
    functionality in two ways: by encapsulating the interception addition and
    removal in methods and by providing a context manager that automates the
    process of addition and removal.

    Each Interceptor subclass is associated with a specific http library.

    Each class may be passed a url or a host and a port. If no args are passed
    a hostname will be automatically generated and the resulting url will be
    returned by the context manager.
    """

    def __init__(self, app, host=None, port=80, prefix=None, url=None):
        assert app
        if (not host and not url):
            host = str(uuid4())

        self.app = app

        if url:
            self._init_from_url(url)
            self.url = url
        else:
            self.host = host
            self.port = int(port)
            self.script_name = prefix or ''
            self.url = self._url_from_primitives()

        self._module = import_module('.%s' % self.MODULE_NAME,
                                     package='wsgi_intercept')

    def __enter__(self):
        self.install_intercept()
        return self.url

    def __exit__(self, exc_type, value, traceback):
        self.uninstall_intercept()

    def _url_from_primitives(self):
        if self.port == 443:
            scheme = 'https'
        else:
            scheme = 'http'

        if self.port and self.port not in [443, 80]:
            port = ':%s' % self.port
        else:
            port = ''
        netloc = self.host + port

        return urlparse.urlunsplit((scheme, netloc, self.script_name,
                                    None, None))

    def _init_from_url(self, url):
        port = None
        parsed_url = urlparse.urlsplit(url)
        if ':' in parsed_url.netloc:
            host, port = parsed_url.netloc.split(':')
        else:
            host = parsed_url.netloc
        if not port:
            if parsed_url.scheme == 'https':
                port = 443
            else:
                port = 80
        path = parsed_url.path
        if path == '/' or not path:
            self.script_name = ''
        else:
            self.script_name = path
        self.host = host
        self.port = int(port)

    def install_intercept(self):
        self._module.install()
        wsgi_intercept.add_wsgi_intercept(self.host, self.port, self.app,
                                          script_name=self.script_name)

    def uninstall_intercept(self):
        remaining = wsgi_intercept.remove_wsgi_intercept(self.host, self.port)
        # Only remove the module's class overrides if there are no intercepts
        # left. Otherwise nested context managers cannot work.
        if not remaining:
            self.uninstall_module()

    def uninstall_module(self):
        self._module.uninstall()


class HttpClientInterceptor(Interceptor):
    """Interceptor for httplib and http.client."""

    MODULE_NAME = 'http_client_intercept'


class Httplib2Interceptor(Interceptor):
    """Interceptor for httplib2."""

    MODULE_NAME = 'httplib2_intercept'


class RequestsInterceptor(Interceptor):
    """Interceptor for requests."""

    MODULE_NAME = 'requests_intercept'


class Urllib3Interceptor(Interceptor):
    """Interceptor for requests."""

    MODULE_NAME = 'urllib3_intercept'


class UrllibInterceptor(Interceptor):
    """Interceptor for urllib2 and urllib.request."""

    MODULE_NAME = 'urllib_intercept'
