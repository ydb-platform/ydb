import uuid
from gevent import spawn, wait
from gevent.event import Event
from gevent.monkey import patch_all
from gevent.queue import Queue, Empty
from gevent.select import select

from werkzeug.exceptions import HTTPException

from .websocket import WebSocket, WebSocketMiddleware
from ._uwsgi import uwsgi


class GeventWebSocketClient(object):
    def __init__(self, environ, fd, send_event, send_queue, recv_event,
                 recv_queue, timeout=5):
        self.environ    = environ
        self.fd         = fd
        self.send_event = send_event
        self.send_queue = send_queue
        self.recv_event = recv_event
        self.recv_queue = recv_queue
        self.timeout    = timeout
        self.id         = str(uuid.uuid1())
        self.connected  = True

    def send(self, msg, binary=True):
        if binary:
            return self.send_binary(msg)
        self.send_queue.put(msg)
        self.send_event.set()

    def send_binary(self, msg):
        self.send_queue.put(msg)
        self.send_event.set()

    def receive(self):
        return self.recv()

    def recv(self):
        return self.recv_queue.get()

    def close(self):
        self.connected = False


class GeventWebSocketMiddleware(WebSocketMiddleware):
    client = GeventWebSocketClient

    def __call__(self, environ, start_response):
        urls = self.websocket.url_map.bind_to_environ(environ)
        try:
            endpoint, args = urls.match()
            handler = self.websocket.view_functions[endpoint]
        except HTTPException:
            handler = None

        if not handler or 'HTTP_SEC_WEBSOCKET_KEY' not in environ:
            return self.wsgi_app(environ, start_response)

        # do handshake
        uwsgi.websocket_handshake(environ['HTTP_SEC_WEBSOCKET_KEY'],
                                  environ.get('HTTP_ORIGIN', ''))

        # setup events
        send_event = Event()
        send_queue = Queue()

        recv_event = Event()
        recv_queue = Queue()

        # create websocket client
        client = self.client(environ, uwsgi.connection_fd(), send_event,
                             send_queue, recv_event, recv_queue,
                             self.websocket.timeout)

        # spawn handler
        handler = spawn(handler, client, **args)

        # spawn recv listener
        def listener(client):
            # wait max `client.timeout` seconds to allow ping to be sent
            select([client.fd], [], [], client.timeout)
            recv_event.set()
        listening = spawn(listener, client)

        while True:
            if not client.connected:
                recv_queue.put(None)
                listening.kill()
                handler.join(client.timeout)
                return ''

            # wait for event to draw our attention
            wait([handler, send_event, recv_event], None, 1)

            # handle send events
            if send_event.is_set():
                try:
                    while True:
                        uwsgi.websocket_send(send_queue.get_nowait())
                except Empty:
                    send_event.clear()
                except IOError:
                    client.connected = False

            # handle receive events
            elif recv_event.is_set():
                recv_event.clear()
                try:
                    message = True
                    # More than one message may have arrived, so keep reading
                    # until an empty message is read. Note that  select()
                    # won't register after we've read a byte until all the
                    # bytes are read, make certain to read all the data.
                    # Experimentally, not putting the final empty message
                    # into the queue caused websocket timeouts; theoretically
                    # this code can skip writing the empty message but clients
                    # should be able to ignore it anyway.
                    while message:
                        message = uwsgi.websocket_recv_nb()
                        recv_queue.put(message)
                    listening = spawn(listener, client)
                except IOError:
                    client.connected = False

            # handler done, we're outta here
            elif handler.ready():
                listening.kill()
                return ''


class GeventWebSocket(WebSocket):
    middleware = GeventWebSocketMiddleware

    def init_app(self, app):
        aggressive = app.config.get('UWSGI_WEBSOCKET_AGGRESSIVE_PATCH', True)
        patch_all(aggressive=aggressive)
        super(GeventWebSocket, self).init_app(app)
