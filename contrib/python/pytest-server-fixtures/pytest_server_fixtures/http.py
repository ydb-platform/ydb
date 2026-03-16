from __future__ import print_function

import os
import socket
import logging
import time
import sys

import pytest
import requests
from contextlib import contextmanager
from six.moves import http_client

from pytest_shutil.env import unset_env
from pytest_server_fixtures import CONFIG
from .base import TestServer


log = logging.getLogger(__name__)


@pytest.yield_fixture
def simple_http_test_server():
    """ Function-scoped py.test fixture to serve up a directory via HTTP.
    """
    with SimpleHTTPTestServer() as s:
        s.start()
        yield s


class HTTPTestServer(TestServer):
    # Bind to all sockets when creating the web-server, for selenium tests
    hostname = '0.0.0.0'

    def __init__(self, uri=None, **kwargs):
        self._uri = uri
        super(HTTPTestServer, self).__init__(**kwargs)

    @property
    def uri(self):
        if self._uri:
            return self._uri
        return "http://%s:%s" % (self.hostname, self.port)

    @contextmanager
    def handle_proxy(self):
        if CONFIG.disable_proxy:
            with unset_env(['http_proxy', 'https_proxy', 'HTTP_PROXY', ' HTTPS_PROXY']):
                yield
        else:
            yield

    def check_server_up(self):
        """ Check the server is up by polling self.uri
        """
        try:
            log.debug('accessing URL: {0}'.format(self.uri))
            with self.handle_proxy():
                resp = requests.get(self.uri)
            acceptable_codes = (200, 403)  # 403 server probably running in secure mode...
            log.debug('Querying %s received response code %s' % (self.uri, resp.status_code))
            return resp.status_code in acceptable_codes
        except requests.ConnectionError as e:
            log.debug("Server not up yet (%s).." % e)
            return False

    def get(self, path, as_json=False, attempts=25):
        """ Queries the server using requests.GET and returns the response object. 
        
        Parameters
        ----------
        path :  `str`
            Path to the resource, relative to 'http://hostname:port/'
        as_json :  `bool`
            Returns the json object if True. Defaults to False.
        attempts: `int`
            This function will retry up to `attempts` times on connection errors, to handle 
            the server still waking up. Defaults to 25.
        """
        e = None
        for i in range(attempts):
            try:
                with self.handle_proxy():
                    returned = requests.get('http://%s:%d/%s' % (self.hostname, self.port, path))
                return returned.json() if as_json else returned
            except (http_client.BadStatusLine, requests.ConnectionError) as e:
                time.sleep(int(i) / 10)
                pass
        raise e

    def post(self, path, data=None, attempts=25, as_json=False, headers=None):
        """ Posts data to the server using requests.POST and returns the response object. 
        
        Parameters
        ----------
        path :  `str`
            Path to the resource, relative to 'http://hostname:port/'
        as_json :  `bool`
            Returns the json response if True. Defaults to False.
        attempts: `int`
            This function will retry up to `attempts` times on connection errors, to handle 
            the server still waking up. Defaults to 25.
        headers: `dict`
            Optional HTTP headers.
        """
        e = None
        for i in range(attempts):
            try:
                with self.handle_proxy():
                    returned = requests.post('http://%s:%d/%s' % (self.hostname, self.port, path), data=data, headers=headers)
                return returned.json() if as_json else returned
            except (http_client.BadStatusLine, requests.ConnectionError) as e:
                time.sleep(int(i) / 10)
                pass
        raise e


class SimpleHTTPTestServer(HTTPTestServer):
    """A Simple HTTP test server that serves up a folder of files over the web."""

    def __init__(self, workspace=None, delete=None, **kwargs):
        kwargs.pop("hostname", None)  # User can't set the hostname it is always 0.0.0.0
        # If we don't pass hostname="0.0.0.0" to our superclass's initialiser then the cleanup
        # code in kill won't work correctly. We don't set self.hostname however as we want our
        # uri property to still be correct.
        super(SimpleHTTPTestServer, self).__init__(workspace=workspace, delete=delete, hostname="0.0.0.0", **kwargs)
        self.cwd = self.document_root

    @property
    def uri(self):
        if self._uri:
            return self._uri
        return "http://%s:%s" % (socket.gethostname(), self.port)

    @property
    def run_cmd(self):
        http_server = 'http.server' if sys.version_info >= (3,0) else 'SimpleHTTPServer'
        return ["python", "-m", http_server, str(self.port)]

    @property
    def document_root(self):
        """This is the folder of files served up by this SimpleHTTPServer"""
        file_dir = os.path.join(str(self.workspace), "files")
        if not os.path.exists(file_dir):
            os.mkdir(file_dir)
        return file_dir
