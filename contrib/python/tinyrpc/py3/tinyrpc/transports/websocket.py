#!/usr/bin/env python
# -*- coding: utf-8 -*-

import queue
from typing import Callable, Tuple, Any

from . import ServerTransport
from geventwebsocket.resource import WebSocketApplication, Resource


class WSServerTransport(ServerTransport):
    """
    Requires :py:mod:`geventwebsocket`.

    Due to the nature of WS, this transport has a few peculiarities: It must
    be run in a thread, greenlet or some other form of concurrent execution
    primitive.

    This is due to
    :py:attr:`~tinyrpc.transports.websocket.WSServerTransport.handle` which is
    a :py:class:`geventwebsocket.resource.Resource` that joins a wsgi handler
    for the / and a WebSocket handler for the /ws path. These resource is
    used in combination with a :py:class:`geventwebsocket.server.WebSocketServer`
    that blocks while waiting for a call to
    :py:func:`~tinyrpc.transports.wsgi.WSServerTransport.send_reply`.

    The parameter ``queue_class`` must be used to supply a proper queue class
    for the chosen concurrency mechanism (i.e. when using :py:mod:`gevent`,
    set it to :py:class:`gevent.queue.Queue`).

    :param queue_class: The queue class to use.
    :param wsgi_handler: Can be used to change the standard response to a
        http request to the /
    """
    def __init__(
            self,
            queue_class: queue.Queue = queue.Queue,
            wsgi_handler: Callable[[], str] = None
    ) -> None:
        self._queue_class = queue_class
        self.messages = queue_class()

        def static_wsgi_app(environ, start_response) -> str:
            start_response("200 OK", [("Content-Type", "text/html")])
            return 'Ready for WebSocket connection in /ws'

        self.handle = Resource({
            '/':
            static_wsgi_app if wsgi_handler is None else wsgi_handler,
            '/ws':
            WSApplicationFactory(self.messages, queue_class)
        })

    def receive_message(self) -> Tuple[Any, bytes]:
        return self.messages.get()

    def send_reply(self, context: Any, reply: bytes) -> None:
        context.put(reply)


class WSApplicationFactory(object):
    """
    Creates WebSocketApplications with a messages queue and the queue_class
    needed for the communication with the WSServerTransport.
    """
    def __init__(self, messages: queue.Queue, queue_class):
        self.messages = messages
        self._queue_class = queue_class

    def __call__(self, ws):
        """
        The fake __init__ for the WSApplication
        """
        app = WSApplication(ws)
        app.messages = self.messages
        app._queue_class = self._queue_class
        return app

    @classmethod
    def protocol(cls):
        return WebSocketApplication.protocol()


class WSApplication(WebSocketApplication):
    """
    This class is the bridge between the WSServerTransport and the WebSocket
    protocol implemented by
    :py:class:`geventwebsocket.resource.WebSocketApplication`
    """
    def on_message(self, msg, *args, **kwargs):
        # create new context
        context = self._queue_class()
        self.messages.put((context, msg))
        response = context.get()
        self.ws.send(response, *args, **kwargs)
