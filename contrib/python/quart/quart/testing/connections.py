from __future__ import annotations

import asyncio
from types import TracebackType
from typing import Any, AnyStr, Awaitable, List, Optional, Tuple, TYPE_CHECKING

from hypercorn.typing import ASGIReceiveEvent, ASGISendEvent, HTTPScope, WebsocketScope
from werkzeug.datastructures import Headers

from ..json import dumps, loads
from ..utils import decode_headers
from ..wrappers import Response

if TYPE_CHECKING:
    from ..app import Quart  # noqa


class HTTPDisconnectError(Exception):
    pass


class WebsocketDisconnectError(Exception):
    pass


class WebsocketResponseError(Exception):
    def __init__(self, response: Response) -> None:
        super().__init__()
        self.response = response


class TestHTTPConnection:
    def __init__(self, app: Quart, scope: HTTPScope, _preserve_context: bool = False) -> None:
        self.app = app
        self.headers: Optional[Headers] = None
        self.push_promises: List[Tuple[str, Headers]] = []
        self.response_data = bytearray()
        self.scope = scope
        self.status_code: Optional[int] = None
        self._preserve_context = _preserve_context
        self._send_queue: asyncio.Queue = asyncio.Queue()
        self._receive_queue: asyncio.Queue = asyncio.Queue()
        self._task: Awaitable[None] = None

    async def send(self, data: bytes) -> None:
        await self._send_queue.put({"type": "http.request", "body": data, "more_body": True})

    async def send_complete(self) -> None:
        await self._send_queue.put({"type": "http.request", "body": b"", "more_body": False})

    async def receive(self) -> bytes:
        data = await self._receive_queue.get()
        if isinstance(data, Exception):
            raise data
        else:
            return data

    async def disconnect(self) -> None:
        await self._send_queue.put({"type": "http.disconnect"})

    async def __aenter__(self) -> "TestHTTPConnection":
        self._task = asyncio.ensure_future(
            self.app(self.scope, self._asgi_receive, self._asgi_send)
        )
        return self

    async def __aexit__(self, exc_type: type, exc_value: BaseException, tb: TracebackType) -> None:
        if exc_type is not None:
            await self.disconnect()
        await self._task
        while not self._receive_queue.empty():
            data = await self._receive_queue.get()
            if isinstance(data, bytes):
                self.response_data.extend(data)
            elif not isinstance(data, HTTPDisconnectError):
                raise data

    async def as_response(self) -> Response:
        while not self._receive_queue.empty():
            data = await self._receive_queue.get()
            if isinstance(data, bytes):
                self.response_data.extend(data)
        return self.app.response_class(bytes(self.response_data), self.status_code, self.headers)

    async def _asgi_receive(self) -> ASGIReceiveEvent:
        return await self._send_queue.get()

    async def _asgi_send(self, message: ASGISendEvent) -> None:
        if message["type"] == "http.response.start":
            self.headers = decode_headers(message["headers"])
            self.status_code = message["status"]
        elif message["type"] == "http.response.body":
            await self._receive_queue.put(message["body"])
        elif message["type"] == "http.response.push":
            self.push_promises.append((message["path"], decode_headers(message["headers"])))
        elif message["type"] == "http.disconnect":
            await self._receive_queue.put(HTTPDisconnectError())


class TestWebsocketConnection:
    def __init__(self, app: Quart, scope: WebsocketScope) -> None:
        self.accepted = False
        self.app = app
        self.headers: Optional[Headers] = None
        self.response_data = bytearray()
        self.scope = scope
        self.status_code: Optional[int] = None
        self._send_queue: asyncio.Queue = asyncio.Queue()
        self._receive_queue: asyncio.Queue = asyncio.Queue()
        self._task: Awaitable[None] = None

    async def __aenter__(self) -> "TestWebsocketConnection":
        self._task = asyncio.ensure_future(
            self.app(self.scope, self._asgi_receive, self._asgi_send)
        )
        return self

    async def __aexit__(self, exc_type: type, exc_value: BaseException, tb: TracebackType) -> None:
        await self.disconnect()
        await self._task
        while not self._receive_queue.empty():
            data = await self._receive_queue.get()
            if isinstance(data, Exception) and not isinstance(data, WebsocketDisconnectError):
                raise data

    async def receive(self) -> AnyStr:
        data = await self._receive_queue.get()
        if isinstance(data, Exception):
            raise data
        else:
            return data

    async def send(self, data: AnyStr) -> None:
        if isinstance(data, str):
            await self._send_queue.put({"type": "websocket.receive", "text": data})
        else:
            await self._send_queue.put({"type": "websocket.receive", "bytes": data})

    async def receive_json(self) -> Any:
        data = await self.receive()
        return loads(data)

    async def send_json(self, data: Any) -> None:
        raw = dumps(data)
        await self.send(raw)

    async def close(self, code: int) -> None:
        await self._send_queue.put({"type": "websocket.close", "code": code})

    async def disconnect(self) -> None:
        await self._send_queue.put({"type": "websocket.disconnect"})

    async def _asgi_receive(self) -> ASGIReceiveEvent:
        return await self._send_queue.get()

    async def _asgi_send(self, message: ASGISendEvent) -> None:
        if message["type"] == "websocket.accept":
            self.accepted = True
        elif message["type"] == "websocket.send":
            await self._receive_queue.put(message.get("bytes") or message.get("text"))
        elif message["type"] == "websocket.http.response.start":
            self.headers = decode_headers(message["headers"])
            self.status_code = message["status"]
        elif message["type"] == "websocket.http.response.body":
            self.response_data.extend(message["body"])
            if not message.get("more_body", False):
                await self._receive_queue.put(
                    WebsocketResponseError(
                        self.app.response_class(
                            bytes(self.response_data), self.status_code, self.headers
                        )
                    )
                )
        elif message["type"] == "websocket.close":
            await self._receive_queue.put(WebsocketDisconnectError(message.get("code", 1000)))
