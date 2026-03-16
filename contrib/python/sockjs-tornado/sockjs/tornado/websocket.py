import tornado
from tornado import escape, gen, websocket

try:
    from urllib.parse import urlparse # py3
except ImportError:
    from urlparse import urlparse # py2


class SockJSWebSocketHandler(websocket.WebSocketHandler):
    if tornado.version_info[0] == 4 and tornado.version_info[1] > 1:
        def get_compression_options(self):
            # let tornado use compression when Sec-WebSocket-Extensions:permessage-deflate is provided
            return {}

    def check_origin(self, origin):
        # let tornado first check if connection from the same domain
        same_domain = super(SockJSWebSocketHandler, self).check_origin(origin)
        if same_domain:
            return True

        # this is cross-origin connection - check using SockJS server settings
        allow_origin = self.server.settings.get("websocket_allow_origin", "*")
        if allow_origin == "*":
            return True
        else:
            parsed_origin = urlparse(origin)
            origin = parsed_origin.netloc
            origin = origin.lower()
            return origin in allow_origin

    def abort_connection(self):
        if self.ws_connection:
            self.ws_connection._abort()

    @gen.coroutine
    def _execute(self, transforms, *args, **kwargs):
        self._transforms = transforms
        # Websocket only supports GET method
        if self.request.method != "GET":
            self.set_status(405)
            self.set_header("Allow", "GET")
            self.set_header("Connection", "Close")
            self.finish("WebSocket only supports GET requests.")
            return

        # Upgrade header should be present and should be equal to WebSocket
        if self.request.headers.get("Upgrade", "").lower() != "websocket":
            self.set_status(400)
            self.set_header("Connection", "Close")
            self.finish("Can \"Upgrade\" only to \"WebSocket\".")
            return

        # Connection header should be upgrade. Some proxy servers/load balancers
        # might mess with it.
        headers = self.request.headers
        connection = map(lambda s: s.strip().lower(), headers.get("Connection", "").split(","))
        if "upgrade" not in connection:
            self.set_status(400)
            self.set_header("Connection", "Close")
            self.finish("\"Connection\" must be \"Upgrade\".")
            return

        yield super(SockJSWebSocketHandler, self)._execute(transforms, *args, **kwargs)
