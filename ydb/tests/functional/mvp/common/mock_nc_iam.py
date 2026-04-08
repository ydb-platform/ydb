import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs

from library.python.port_manager import PortManager


class BaseTestHandler(BaseHTTPRequestHandler):
    server_version = "TestHTTP/1.0"
    protocol_version = "HTTP/1.1"

    def log_message(self, format, *args):
        return


class MockNcIamServer(ThreadingHTTPServer):
    def __init__(self, server_address):
        super().__init__(server_address, MockNcIamHandler)
        self.token_requests = []
        self.exchange_requests = []
        self.impersonation_requests = []


class MockNcIamHandler(BaseTestHandler):
    def do_POST(self):
        content_length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(content_length).decode()
        parsed = parse_qs(body)

        if self.path == "/oauth/token":
            self.server.token_requests.append({
                "headers": dict(self.headers.items()),
                "body": parsed,
            })
            payload = b'{"access_token":"session_token_value","token_type":"bearer","expires_in":3600}'
        elif self.path == "/oauth2/session/exchange":
            self.server.exchange_requests.append({
                "headers": dict(self.headers.items()),
                "body": parsed,
            })
            payload = b'{"access_token":"protected_page_iam_token","token_type":"bearer","expires_in":3600}'
        elif self.path == "/oauth2/impersonation/impersonate":
            self.server.impersonation_requests.append({
                "headers": dict(self.headers.items()),
                "body": parsed,
            })
            payload = b'{"impersonation":"impersonation_token","expires_in":43200}'
        else:
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


class MockNcIamService:
    def __init__(self, server):
        self._server = server
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    @property
    def endpoint(self):
        host, port = self._server.server_address
        return f"http://{host}:{port}"

    @property
    def token_requests(self):
        return self._server.token_requests

    @property
    def exchange_requests(self):
        return self._server.exchange_requests

    @property
    def impersonation_requests(self):
        return self._server.impersonation_requests

    def start(self):
        self._thread.start()
        return self

    def stop(self):
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)


@contextmanager
def started_mock_nc_iam():
    with PortManager() as port_manager:
        auth_port = port_manager.get_port()
        auth_service = MockNcIamService(MockNcIamServer(("localhost", auth_port))).start()
        try:
            yield auth_service
        finally:
            auth_service.stop()
