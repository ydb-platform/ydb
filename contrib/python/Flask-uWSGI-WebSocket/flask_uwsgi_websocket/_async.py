from .websocket import WebSocket, WebSocketClient, WebSocketMiddleware
from ._uwsgi import uwsgi


class AsyncWebSocketClient(WebSocketClient):
    def receive(self):
        while True:
            uwsgi.wait_fd_read(self.fd, self.timeout)
            uwsgi.suspend()
            if uwsgi.ready_fd() == self.fd:
                return uwsgi.websocket_recv_nb()


class AsyncWebSocketMiddleware(WebSocketMiddleware):
    client = AsyncWebSocketClient


class AsyncWebSocket(WebSocket):
    middleware = AsyncWebSocketMiddleware
