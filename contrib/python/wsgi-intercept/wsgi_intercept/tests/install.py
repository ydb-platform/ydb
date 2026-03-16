import os
import pytest
import wsgi_intercept


skipnetwork = pytest.mark.skipif(os.environ.get(
    'WSGI_INTERCEPT_SKIP_NETWORK', 'False').lower() == 'true',
    reason="skip network requested"
)


class BaseInstalledApp(object):
    def __init__(self, app, host, port=80, script_name='',
                 install=None, uninstall=None, proxy=None):
        self.app = app
        self.host = host
        self.port = port
        self.script_name = script_name
        self._install = install or (lambda: None)
        self._uninstall = uninstall or (lambda: None)
        self._hits = 0
        self._internals = {}
        self._proxy = proxy

    def __call__(self, environ, start_response):
        self._hits += 1
        self._internals = environ
        return self.app(environ, start_response)

    def success(self):
        return self._hits > 0

    def get_internals(self):
        return self._internals

    def install_wsgi_intercept(self):
        wsgi_intercept.add_wsgi_intercept(
            self.host, self.port, self.factory, script_name=self.script_name)

    def uninstall_wsgi_intercept(self):
        wsgi_intercept.remove_wsgi_intercept(self.host, self.port)

    def install(self):
        if self._proxy:
            os.environ['http_proxy'] = self._proxy
        self._install()
        self.install_wsgi_intercept()

    def uninstall(self):
        if self._proxy:
            del os.environ['http_proxy']
        self.uninstall_wsgi_intercept()
        self._uninstall()

    def factory(self):
        return self

    def __enter__(self):
        self.install()
        return self

    def __exit__(self, *args, **kwargs):
        self.uninstall()


def installer_class(module=None, install=None, uninstall=None):
    if module:
        install = install or getattr(module, 'install', None)
        uninstall = uninstall or getattr(module, 'uninstall', None)

    class InstalledApp(BaseInstalledApp):
        def __init__(self, app, host, port=80, script_name='', proxy=None):
            BaseInstalledApp.__init__(
                self, app=app, host=host, port=port, script_name=script_name,
                install=install, uninstall=uninstall, proxy=proxy)

    return InstalledApp
