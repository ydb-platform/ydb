from __future__ import annotations

import asyncio
from types import TracebackType
from typing import Awaitable, TYPE_CHECKING

from hypercorn.typing import ASGIReceiveEvent, ASGISendEvent, LifespanScope

from ..typing import TestClientProtocol

if TYPE_CHECKING:
    from ..app import Quart  # noqa

DEFAULT_TIMEOUT = 6


class LifespanError(Exception):
    pass


class TestApp:
    def __init__(
        self,
        app: "Quart",
        startup_timeout: int = DEFAULT_TIMEOUT,
        shutdown_timeout: int = DEFAULT_TIMEOUT,
    ) -> None:
        self.app = app
        self.startup_timeout = startup_timeout
        self.shutdown_timeout = shutdown_timeout
        self._startup = asyncio.Event()
        self._shutdown = asyncio.Event()
        self._app_queue: asyncio.Queue = asyncio.Queue()
        self._task: Awaitable[None] = None

    def test_client(self) -> TestClientProtocol:
        return self.app.test_client()

    async def startup(self) -> None:
        scope: LifespanScope = {"type": "lifespan", "asgi": {"spec_version": "2.0"}}
        self._task = asyncio.ensure_future(self.app(scope, self._asgi_receive, self._asgi_send))
        await self._app_queue.put({"type": "lifespan.startup"})
        await asyncio.wait_for(self._startup.wait(), timeout=self.startup_timeout)
        if self._task.done():
            # This will re-raise any exceptions in the task
            await self._task

    async def shutdown(self) -> None:
        await self._app_queue.put({"type": "lifespan.shutdown"})
        await asyncio.wait_for(self._shutdown.wait(), timeout=self.shutdown_timeout)
        await self._task

    async def __aenter__(self) -> "TestApp":
        await self.startup()
        return self

    async def __aexit__(self, exc_type: type, exc_value: BaseException, tb: TracebackType) -> None:
        await self.shutdown()

    async def _asgi_receive(self) -> ASGIReceiveEvent:
        return await self._app_queue.get()

    async def _asgi_send(self, message: ASGISendEvent) -> None:
        if message["type"] == "lifespan.startup.complete":
            self._startup.set()
        elif message["type"] == "lifespan.shutdown.complete":
            self._shutdown.set()
        elif message["type"] == "lifespan.startup.failed":
            self._startup.set()
            raise LifespanError(f"Error during startup {message['message']}")
        elif message["type"] == "lifespan.shutdown.failed":
            self._shutdown.set()
            raise LifespanError(f"Error during shutdown {message['message']}")
