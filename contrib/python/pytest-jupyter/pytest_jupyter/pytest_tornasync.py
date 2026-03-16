"""Vendored fork of pytest_tornasync from
https://github.com/eukaryote/pytest-tornasync/blob/9f1bdeec3eb5816e0183f975ca65b5f6f29fbfbb/src/pytest_tornasync/plugin.py
"""

import asyncio
from contextlib import closing

try:
    import tornado.ioloop
    import tornado.testing
    from tornado.simple_httpclient import SimpleAsyncHTTPClient
except ImportError:
    SimpleAsyncHTTPClient = object  # type:ignore[assignment,misc]

import pytest

# mypy: disable-error-code="no-untyped-call"
# Bring in local plugins.
from pytest_jupyter.jupyter_core import *  # noqa: F403


@pytest.fixture
def io_loop(jp_asyncio_loop):
    """Get the current tornado event loop."""
    return tornado.ioloop.IOLoop.current()


@pytest.fixture
def http_server(jp_asyncio_loop, http_server_port, jp_web_app):
    """Start a tornado HTTP server that listens on all available interfaces."""

    async def get_server():
        """Get a server asynchronously."""
        server = tornado.httpserver.HTTPServer(jp_web_app)
        server.add_socket(http_server_port[0])
        return server

    server = jp_asyncio_loop.run_until_complete(get_server())
    yield server
    server.stop()

    if hasattr(server, "close_all_connections"):
        try:
            jp_asyncio_loop.run_until_complete(server.close_all_connections())
        except asyncio.TimeoutError:
            pass

    http_server_port[0].close()


@pytest.fixture
def http_server_port():
    """
    Port used by `http_server`.
    """
    return tornado.testing.bind_unused_port()


@pytest.fixture
def http_server_client(http_server, jp_asyncio_loop):
    """
    Create an asynchronous HTTP client that can fetch from `http_server`.
    """

    async def get_client():
        """Get a client."""
        return AsyncHTTPServerClient(http_server=http_server)

    client = jp_asyncio_loop.run_until_complete(get_client())
    with closing(client) as context:
        yield context


class AsyncHTTPServerClient(SimpleAsyncHTTPClient):
    """An async http server client."""

    def initialize(self, *, http_server=None):  # type: ignore[override]
        """Initialize the client."""
        super().initialize()
        self._http_server = http_server

    def fetch(self, path, **kwargs):  # type: ignore[override]
        """
        Fetch `path` from test server, passing `kwargs` to the `fetch`
        of the underlying `SimpleAsyncHTTPClient`.
        """
        return super().fetch(self.get_url(path), **kwargs)

    def get_protocol(self):
        """Get the protocol for the client."""
        return "http"

    def get_http_port(self):
        """Get a port for the client."""
        for sock in self._http_server._sockets.values():
            return sock.getsockname()[1]
        return None

    def get_url(self, path):
        """Get the url for the client."""
        return f"{self.get_protocol()}://127.0.0.1:{self.get_http_port()}{path}"
