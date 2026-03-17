from __future__ import annotations

import asyncio
import warnings
from functools import partial
from typing import AnyStr, cast, List, Optional, Set, TYPE_CHECKING, Union
from urllib.parse import urlparse

from hypercorn.typing import (
    ASGIReceiveCallable,
    ASGISendCallable,
    HTTPResponseBodyEvent,
    HTTPResponseStartEvent,
    HTTPScope,
    LifespanScope,
    LifespanShutdownCompleteEvent,
    LifespanShutdownFailedEvent,
    LifespanStartupCompleteEvent,
    LifespanStartupFailedEvent,
    WebsocketAcceptEvent,
    WebsocketCloseEvent,
    WebsocketResponseBodyEvent,
    WebsocketResponseStartEvent,
    WebsocketScope,
)
from werkzeug.datastructures import Headers
from werkzeug.wrappers import Response as WerkzeugResponse

from .debug import traceback_response
from .signals import websocket_received, websocket_sent
from .utils import encode_headers
from .wrappers import Request, Response, Websocket  # noqa: F401

if TYPE_CHECKING:
    from .app import Quart  # noqa: F401


class ASGIHTTPConnection:
    def __init__(self, app: "Quart", scope: HTTPScope) -> None:
        self.app = app
        self.scope = scope

    async def __call__(self, receive: ASGIReceiveCallable, send: ASGISendCallable) -> None:
        request = self._create_request_from_scope(send)
        receiver_task = asyncio.ensure_future(self.handle_messages(request, receive))
        handler_task = asyncio.ensure_future(self.handle_request(request, send))
        done, pending = await asyncio.wait(
            [handler_task, receiver_task], return_when=asyncio.FIRST_COMPLETED
        )
        await _cancel_tasks(pending)
        _raise_exceptions(done)

    async def handle_messages(self, request: Request, receive: ASGIReceiveCallable) -> None:
        while True:
            message = await receive()
            if message["type"] == "http.request":
                request.body.append(message.get("body", b""))
                if not message.get("more_body", False):
                    request.body.set_complete()
            elif message["type"] == "http.disconnect":
                return

    def _create_request_from_scope(self, send: ASGISendCallable) -> Request:
        headers = Headers()
        headers["Remote-Addr"] = (self.scope.get("client") or ["<local>"])[0]
        for name, value in self.scope["headers"]:
            headers.add(name.decode("latin1").title(), value.decode("latin1"))
        if self.scope["http_version"] < "1.1":
            headers.setdefault("Host", self.app.config["SERVER_NAME"] or "")

        path = self.scope["path"]
        path = path if path[0] == "/" else urlparse(path).path

        return self.app.request_class(
            self.scope["method"],
            self.scope["scheme"],
            path,
            self.scope["query_string"],
            headers,
            self.scope.get("root_path", ""),
            self.scope["http_version"],
            max_content_length=self.app.config["MAX_CONTENT_LENGTH"],
            body_timeout=self.app.config["BODY_TIMEOUT"],
            send_push_promise=partial(self._send_push_promise, send),
            scope=self.scope,
        )

    async def handle_request(self, request: Request, send: ASGISendCallable) -> None:
        try:
            response = await self.app.handle_request(request)
        except Exception:
            if self.app.propagate_exceptions:
                response = await traceback_response()
            else:
                raise

        if isinstance(response, Response) and response.timeout != Ellipsis:
            timeout = cast(Optional[float], response.timeout)
        else:
            timeout = self.app.config["RESPONSE_TIMEOUT"]
        try:
            await asyncio.wait_for(self._send_response(send, response), timeout=timeout)
        except asyncio.TimeoutError:
            pass

    async def _send_response(
        self, send: ASGISendCallable, response: Union[Response, WerkzeugResponse]
    ) -> None:
        await send(
            cast(
                HTTPResponseStartEvent,
                {
                    "type": "http.response.start",
                    "status": response.status_code,
                    "headers": encode_headers(response.headers),
                },
            )
        )

        if isinstance(response, WerkzeugResponse):
            for data in response.response:
                body = data.encode(response.charset) if isinstance(data, str) else data
                await send(
                    cast(
                        HTTPResponseBodyEvent,
                        {"type": "http.response.body", "body": body, "more_body": True},
                    )
                )
        else:
            async with response.response as response_body:
                async for data in response_body:
                    body = data.encode(response.charset) if isinstance(data, str) else data
                    await send(
                        cast(
                            HTTPResponseBodyEvent,
                            {"type": "http.response.body", "body": body, "more_body": True},
                        )
                    )
        await send(
            cast(
                HTTPResponseBodyEvent,
                {"type": "http.response.body", "body": b"", "more_body": False},
            )
        )

    async def _send_push_promise(self, send: ASGISendCallable, path: str, headers: Headers) -> None:
        if "http.response.push" in self.scope.get("extensions", {}):
            await send(
                {"type": "http.response.push", "path": path, "headers": encode_headers(headers)}
            )


class ASGIWebsocketConnection:
    def __init__(self, app: "Quart", scope: WebsocketScope) -> None:
        self.app = app
        self.scope = scope
        self.queue: asyncio.Queue = asyncio.Queue()
        self._accepted = False
        self._closed = False

    async def __call__(self, receive: ASGIReceiveCallable, send: ASGISendCallable) -> None:
        websocket = self._create_websocket_from_scope(send)
        receiver_task = asyncio.ensure_future(self.handle_messages(receive))
        handler_task = asyncio.ensure_future(self.handle_websocket(websocket, send))
        done, pending = await asyncio.wait(
            [handler_task, receiver_task], return_when=asyncio.FIRST_COMPLETED
        )
        await _cancel_tasks(pending)
        _raise_exceptions(done)

    async def handle_messages(self, receive: ASGIReceiveCallable) -> None:
        while True:
            event = await receive()
            if event["type"] == "websocket.receive":
                message = event.get("bytes") or event["text"]
                await websocket_received.send(message)
                await self.queue.put(message)
            elif event["type"] == "websocket.disconnect":
                return

    def _create_websocket_from_scope(self, send: ASGISendCallable) -> Websocket:
        headers = Headers()
        headers["Remote-Addr"] = (self.scope.get("client") or ["<local>"])[0]
        for name, value in self.scope["headers"]:
            headers.add(name.decode("latin1").title(), value.decode("latin1"))

        path = self.scope["path"]
        path = path if path[0] == "/" else urlparse(path).path

        return self.app.websocket_class(
            path,
            self.scope["query_string"],
            self.scope["scheme"],
            headers,
            self.scope.get("root_path", ""),
            self.scope.get("http_version", "1.1"),
            list(self.scope.get("subprotocols", [])),
            self.queue.get,
            partial(self.send_data, send),
            partial(self.accept_connection, send),
            partial(self.close_connection, send),
            scope=self.scope,
        )

    async def handle_websocket(self, websocket: Websocket, send: ASGISendCallable) -> None:
        try:
            response = await self.app.handle_websocket(websocket)
        except Exception:
            if self.app.propagate_exceptions:
                raise
            else:
                response = await traceback_response()

        if response is not None and not self._accepted:
            if "websocket.http.response" in self.scope.get("extensions", {}):
                headers = [
                    (key.lower().encode(), value.encode())
                    for key, value in response.headers.items()
                ]
                await send(
                    cast(
                        WebsocketResponseStartEvent,
                        {
                            "type": "websocket.http.response.start",
                            "status": response.status_code,
                            "headers": headers,
                        },
                    )
                )
                if isinstance(response, WerkzeugResponse):
                    for data in response.response:
                        await send(
                            cast(
                                WebsocketResponseBodyEvent,
                                {
                                    "type": "websocket.http.response.body",
                                    "body": data,
                                    "more_body": True,
                                },
                            )
                        )
                elif isinstance(response, Response):
                    async with response.response as body:
                        async for data in body:
                            await send(
                                cast(
                                    WebsocketResponseBodyEvent,
                                    {
                                        "type": "websocket.http.response.body",
                                        "body": data,
                                        "more_body": True,
                                    },
                                )
                            )
                await send(
                    cast(
                        WebsocketResponseBodyEvent,
                        {"type": "websocket.http.response.body", "body": b"", "more_body": False},
                    )
                )
            elif not self._closed:
                await send(cast(WebsocketCloseEvent, {"type": "websocket.close", "code": 1000}))
        elif self._accepted and not self._closed:
            await send(cast(WebsocketCloseEvent, {"type": "websocket.close", "code": 1000}))

    async def send_data(self, send: ASGISendCallable, data: AnyStr) -> None:
        if isinstance(data, str):
            await send({"type": "websocket.send", "bytes": None, "text": data})
        else:
            await send({"type": "websocket.send", "bytes": data, "text": None})
        await websocket_sent.send(data)

    async def accept_connection(
        self, send: ASGISendCallable, headers: Headers, subprotocol: Optional[str]
    ) -> None:
        if not self._accepted:
            message: WebsocketAcceptEvent = {
                "headers": [],
                "subprotocol": subprotocol,
                "type": "websocket.accept",
            }
            spec_version = _convert_version(self.scope.get("asgi", {}).get("spec_version", "2.0"))
            if spec_version > [2, 0]:
                message["headers"] = encode_headers(headers)
            elif headers:
                warnings.warn("The ASGI Server does not support accept headers, headers not sent")
            self._accepted = True
            await send(message)

    async def close_connection(self, send: ASGISendCallable, code: int, reason: str) -> None:
        if self._closed:
            raise RuntimeError("Cannot close websocket multiple times")

        spec_version = _convert_version(self.scope.get("asgi", {}).get("spec_version", "2.0"))
        if spec_version >= [2, 3]:
            await send({"type": "websocket.close", "code": code, "reason": reason})
        else:
            await send({"type": "websocket.close", "code": code})  # type: ignore
        self._closed = True


class ASGILifespan:
    def __init__(self, app: "Quart", scope: LifespanScope) -> None:
        self.app = app

    async def __call__(self, receive: ASGIReceiveCallable, send: ASGISendCallable) -> None:
        while True:
            event = await receive()
            if event["type"] == "lifespan.startup":
                try:
                    await self.app.startup()
                except Exception as error:
                    await send(
                        cast(
                            LifespanStartupFailedEvent,
                            {"type": "lifespan.startup.failed", "message": str(error)},
                        ),
                    )
                else:
                    await send(
                        cast(LifespanStartupCompleteEvent, {"type": "lifespan.startup.complete"})
                    )
            elif event["type"] == "lifespan.shutdown":
                try:
                    await self.app.shutdown()
                except Exception as error:
                    await send(
                        cast(
                            LifespanShutdownFailedEvent,
                            {"type": "lifespan.shutdown.failed", "message": str(error)},
                        ),
                    )
                else:
                    await send(
                        cast(LifespanShutdownCompleteEvent, {"type": "lifespan.shutdown.complete"}),
                    )
                break


async def _cancel_tasks(tasks: Set[asyncio.Task]) -> None:
    # Cancel any pending, and wait for the cancellation to
    # complete i.e. finish any remaining work.
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    _raise_exceptions(tasks)


def _raise_exceptions(tasks: Set[asyncio.Task]) -> None:
    # Raise any unexpected exceptions
    for task in tasks:
        if not task.cancelled() and task.exception() is not None:
            raise task.exception()


def _convert_version(raw: str) -> List[int]:
    return list(map(int, raw.split(".")))
