import time
from collections.abc import AsyncGenerator
from socket import socketpair

import pytest_asyncio

from pyroute2.plan9.client import Plan9ClientSocket
from pyroute2.plan9.server import Plan9ServerSocket


def test_time():
    return time.time_ns()


class AsyncPlan9Context:

    server = None
    client = None
    shutdown_response = None
    sample_data = b'Pi6raTaXuzohdu7n'

    def __init__(self):
        self.server_sock, self.client_sock = socketpair()
        self.server = Plan9ServerSocket(use_socket=self.server_sock)
        self.client = Plan9ClientSocket(use_socket=self.client_sock)
        self._task = None
        with self.server.filesystem.create('test_file') as i:
            i.data.write(self.sample_data)
        with self.server.filesystem.create('test_time') as i:
            i.metadata.call_on_read = True
            i.register_function(test_time, loader=lambda x: {})

    async def ensure_session(self):
        self._task = await self.server.async_run()
        await self.client.start_session()

    def close(self):
        self._task.cancel()
        self.client.close()
        self.server.close()


@pytest_asyncio.fixture
async def async_p9_context() -> AsyncGenerator[AsyncPlan9Context]:
    ctx = AsyncPlan9Context()
    await ctx.ensure_session()
    yield ctx
    ctx.close()
