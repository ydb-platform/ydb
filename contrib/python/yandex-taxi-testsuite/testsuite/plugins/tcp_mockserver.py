import asyncio
import contextlib
import socket

import pytest

from testsuite.utils import cached_property, net


class Mockserver:
    """TCP/IP mockserver."""

    def __init__(self, server):
        self._handler = None
        self._sockets = tuple(server.sockets)

    async def _client_connected_cb(self, reader, writer):
        try:
            if self._handler is None:
                raise RuntimeError(
                    'No client handler installed, use client_handler()',
                )
            return await self._handler(reader, writer)
        except Exception:
            writer.close()
            pytest.fail('Mockserver handler failure')

    @cached_property
    def sockets(self) -> tuple[socket.socket]:
        """Returns list of server sockets."""
        return self._sockets

    @cached_property
    def address(self) -> tuple[str, int]:
        """
        Returns service address (host, port)
        """
        assert self._sockets
        return self._sockets[0].getsockname()[:2]

    @contextlib.asynccontextmanager
    async def open_connection(self, timeout=10.0):
        """Async context manager creates connection to the service.

        :param timeout: timeout to establish connection.

        Returns pair (read, writer).

        Connection is closed when context manager is done.

        Wrapper around :func:`asyncio.open_connection`


        .. code-block:: python

           async with server.open_connection() as (reader, writer):
               ...
        """
        host, port = self.address
        coro = asyncio.open_connection(host=host, port=port)
        try:
            reader, writer = await asyncio.wait_for(coro, timeout=timeout)
            yield reader, writer
        finally:
            writer.close()

    @contextlib.contextmanager
    def client_handler(self, handler):
        """Context manager to install per-test client handler.

        .. code-block:: python

          async def handle_client(reader, writer):
              writer.write(b'hello\\r\\n')
              await writer.drain()
              writer.close()

          with _tcp_mockserver.client_handler(handle_client):
              ...
        """
        old_handler = self._handler
        try:
            self._handler = handler
            yield
        finally:
            self._handler = old_handler


class ProtocolFactory:
    def __init__(self):
        self.client_handler = None

    def __call__(self):
        if self.client_handler is None:
            pytest.fail('No client handler attached')
        reader = asyncio.StreamReader()
        protocol = asyncio.StreamReaderProtocol(
            reader,
            self.client_handler,
        )
        return protocol

    @contextlib.contextmanager
    def attach_client_handler(self, handler):
        try:
            self.client_handler = handler
            yield
        finally:
            self.client_handler = None


@pytest.fixture(scope='session')
async def create_tcp_mockserver():
    @contextlib.asynccontextmanager
    async def create_mockserver(
        *,
        host='localhost',
        port=0,
        sock=None,
        **kwargs,
    ):
        factory = ProtocolFactory()
        async with net.create_tcp_server(
            factory,
            host=host,
            port=port,
            sock=sock,
            **kwargs,
        ) as server:
            mockserver = Mockserver(server)
            with factory.attach_client_handler(
                mockserver._client_connected_cb,
            ):
                yield mockserver

    return create_mockserver
