from .websocket import WebSocket, WebSocketClient, WebSocketMiddleware
from ._uwsgi import uwsgi
import asyncio
import greenlet
from enum import Enum
from werkzeug.exceptions import HTTPException


class AsyncioWebSocketClient(WebSocketClient):
    def __init__(self, environ, fd, timeout=5, concurrent=None, loop=None):
        super().__init__(environ, fd, timeout)
        self.environ = environ
        self.fd = fd
        self.timeout = timeout
        if loop is None:
            self._loop = asyncio.get_event_loop()
        else:
            self._loop = loop
        self.concurrent = concurrent
        self.connected = True
        self.send_queue = asyncio.Queue()
        self.recv_queue = asyncio.Queue()
        self._loop.add_reader(self.fd, self._recv_ready)
        self._tickhdr = self._loop.call_later(self.timeout, self._recv_ready)
        self.has_msg = False

    def _recv_ready(self):
        self._tickhdr.cancel()
        if self.connected:
            self.has_msg = True
            self._tickhdr = self._loop.call_later(self.timeout, self._recv_ready)
        self.concurrent.switch()

    async def _send_ready(self, f):
        msg = await self.send_queue.get()
        f.set_result(msg)
        f.greenlet.switch()

    async def receive(self):
        msg = await self.a_recv()
        return msg

    async def recv(self):
        msg = await self.a_recv()
        return msg

    def recv_nb(self):
        if self.connected:
            try:
                msg = self.recv_queue.get_nowait()
            except asyncio.QueueEmpty:
                msg = ''
        else:
            msg = None
        return msg

    async def send(self, msg):
        await self.a_send(msg)

    def send_nb(self, msg):
        if self.connected:
            self.send_queue.put_nowait(msg)
        else:
            raise ConnectionError

    async def a_recv(self):
        if self.connected:
            msg = await self.recv_queue.get()
        else:
            msg = None
        return msg

    async def a_send(self, msg):
        await self.send_queue.put(msg)

    def close(self):
        self.connected = False
        self._loop.remove_reader(self.fd)
        self.recv_queue.put_nowait(None)
        self._tickhdr.cancel()


class GreenFuture(asyncio.Future):
    '''
    GreenFuture class from
    https://github.com/unbit/uwsgi/blob/master/tests/websockets_chat_asyncio.py
    '''
    def __init__(self):
        super().__init__()
        self.greenlet = greenlet.getcurrent()
        self.add_done_callback(lambda f: f.greenlet.switch())

    def result(self):
        while True:
            if self.done():
                return super().result()
            self.greenlet.parent.switch()


class AsyncioWebSocketMiddleware(WebSocketMiddleware):
    client = AsyncioWebSocketClient

    def __call__(self, environ, start_response):
        urls = self.websocket.url_map.bind_to_environ(environ)
        try:
            endpoint, args = urls.match()
            handler = self.websocket.view_functions[endpoint]
        except HTTPException:
            handler = None

        if not handler or 'HTTP_SEC_WEBSOCKET_KEY' not in environ:
            return self.wsgi_app(environ, start_response)

        uwsgi.websocket_handshake(environ['HTTP_SEC_WEBSOCKET_KEY'], environ.get('HTTP_ORIGIN', ''))

        client = self.client(environ, uwsgi.connection_fd(), self.websocket.timeout, greenlet.getcurrent())

        assert asyncio.iscoroutinefunction(handler)
        asyncio.Task(asyncio.coroutine(handler)(client, **args))
        f = GreenFuture()
        asyncio.Task(client._send_ready(f))
        try:
            while True:
                f.greenlet.parent.switch()
                if f.done():
                    msg = f.result()
                    uwsgi.websocket_send(msg)
                    f = GreenFuture()
                    asyncio.Task(client._send_ready(f))
                if client.has_msg:
                    client.has_msg = False
                    msg = uwsgi.websocket_recv_nb()
                    while msg:
                        asyncio.Task(client.recv_queue.put(msg))
                        msg = uwsgi.websocket_recv_nb()
        except OSError:
            client.close()
        return []


class AsyncioWebSocket(WebSocket):
    middleware = AsyncioWebSocketMiddleware
