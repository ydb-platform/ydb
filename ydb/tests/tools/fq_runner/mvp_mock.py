from functools import partial
from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import socket


class MvpMockHttpHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def __init__(self, ydb_database, *args, **kwargs):
        self.ydb_database = ydb_database
        super().__init__(*args, **kwargs)

    def _set_headers(self, length):
        self.send_response(200, "OK")
        self.send_header("Content-type", "application/json; charset=utf-8")
        self.send_header("Content-Length", length)
        self.end_headers()

    def do_GET(self):
        endpoint = "{}/?database={}".format(self.ydb_database, "local")
        self.log_message("send response {}".format(endpoint))
        response = json.dumps({"endpoint" : endpoint}).encode("utf-8")
        self._set_headers(len(response))
        self.wfile.write(response)


class HTTPServerIPv6(HTTPServer):
    address_family = socket.AF_INET6


class MvpMockServer:
    def __init__(self, port, ydb_endpoint):
        self.port = port
        self.server = HTTPServerIPv6(('::', self.port), partial(MvpMockHttpHandler, ydb_endpoint))

    def handle_request(self):
        self.server.handle_request()

    def serve_forever(self):
        self.server.serve_forever()
