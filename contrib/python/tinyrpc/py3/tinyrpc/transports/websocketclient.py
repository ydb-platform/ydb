#!/usr/bin/env python
# -*- coding: utf-8 -*-
from typing import Dict

import geventwebsocket as websocket

from . import ClientTransport


class HttpWebSocketClientTransport(ClientTransport):
    """HTTP WebSocket based client transport.

    Requires :py:mod:`websocket-python`. Submits messages to a server using the body of
    an ``HTTP`` ``WebSocket`` message. Replies are taken from the response of the websocket.

    The connection is establish on the ``__init__`` because the protocol is connection oriented,
    you need to close the connection calling the close method.

    :param endpoint: The URL to connect the websocket.
    :param kwargs: Additional parameters for :py:func:`websocket.send`.
    """
    def __init__(self, endpoint: str, **kwargs: Dict):
        self.endpoint = endpoint
        self.request_kwargs = kwargs
        self.ws = websocket.create_connection(self.endpoint, **kwargs)

    def send_message(self, message: bytes, expect_reply: bool = True) -> bytes:
        if not isinstance(message, bytes):
            raise TypeError('message must by of type bytes')
        self.ws.send(message)
        r = self.ws.recv()
        if expect_reply:
            return r

    def close(self) -> None:
        """Terminate the connection.

        Since WebSocket maintains an open connection over multiple calls
        it must be closed explicitly.
        """
        if self.ws is not None:
            self.ws.close()
