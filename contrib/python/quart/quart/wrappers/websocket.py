from __future__ import annotations

import asyncio
from typing import Any, AnyStr, Callable, List, Optional, Union

from hypercorn.typing import WebsocketScope
from werkzeug.datastructures import Headers

from .base import BaseRequestWebsocket
from ..json import dumps, loads


class Websocket(BaseRequestWebsocket):
    def __init__(
        self,
        path: str,
        query_string: bytes,
        scheme: str,
        headers: Headers,
        root_path: str,
        http_version: str,
        subprotocols: List[str],
        receive: Callable,
        send: Callable,
        accept: Callable,
        close: Callable,
        scope: WebsocketScope,
    ) -> None:
        """Create a request object.

        Arguments:
            path: The full unquoted path of the request.
            query_string: The raw bytes for the query string part.
            scheme: The scheme used for the request.
            headers: The request headers.
            root_path: The root path that should be prepended to all
                routes.
            http_version: The HTTP version of the request.
            subprotocols: The subprotocols requested.
            receive: Returns an awaitable of the current data
            accept: Idempotent callable to accept the websocket connection.
            scope: Underlying ASGI scope dictionary.
        """
        super().__init__("GET", scheme, path, query_string, headers, root_path, http_version, scope)
        self._accept = accept
        self._close = close
        self._receive = receive
        self._send = send
        self._subprotocols = subprotocols

    @property
    def requested_subprotocols(self) -> List[str]:
        return self._subprotocols

    async def receive(self) -> AnyStr:
        await self.accept()
        return await self._receive()

    async def send(self, data: AnyStr) -> None:
        # Must allow for the event loop to act if the user has say
        # setup a tight loop sending data over a websocket (as in the
        # example). So yield via the sleep.
        await asyncio.sleep(0)
        await self.accept()
        await self._send(data)

    async def receive_json(self) -> Any:
        data = await self.receive()
        return loads(data)

    async def send_json(self, data: Any) -> None:
        raw = dumps(data)
        await self.send(raw)

    async def accept(
        self, headers: Optional[Union[dict, Headers]] = None, subprotocol: Optional[str] = None
    ) -> None:
        """Manually chose to accept the websocket connection.

        Arguments:
            headers: Additional headers to send with the acceptance
                response.
            subprotocol: The chosen subprotocol, optional.
        """
        if headers is None:
            headers_ = Headers()
        else:
            headers_ = Headers(headers)
        await self._accept(headers_, subprotocol)

    async def close(self, code: int, reason: str = "") -> None:
        await self._close(code, reason)
