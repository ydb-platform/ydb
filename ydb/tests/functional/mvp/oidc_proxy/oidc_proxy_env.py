import os
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs

import requests
import yatest.common

from library.python.port_manager import PortManager

from ydb.tests.library.common.wait_for import wait_for
from ydb.tests.library.harness.daemon import Daemon
from ydb.tests.library.harness.kikimr_runner import KiKiMR


def mvp_oidc_proxy_bin():
    return yatest.common.binary_path("ydb/mvp/oidc_proxy/bin/mvp_oidc_proxy")


def write_oidc_proxy_config(config_path, http_port):
    with open(config_path, "w") as config:
        config.write(
            "\n".join(
                [
                    "generic:",
                    '  access_service_type: "nebius_v1"',
                    "  logging:",
                    "    stderr: true",
                    "  server:",
                    f"    http_port: {http_port}",
                    "  auth:",
                    "    tokens:",
                    '      access_service_type: "nebius_v1"',
                    "      secret_info:",
                    '        - name: "oidc-client-secret"',
                    '          secret: "test-secret"',
                    "",
                    "oidc:",
                    '  client_id: "test-client"',
                    '  secret_name: "oidc-client-secret"',
                    '  session_service_endpoint: "localhost:8655"',
                    '  session_service_token_name: "session-service-token"',
                    '  authorization_server_address: "http://auth.test.net"',
                ]
            )
        )


def write_oidc_proxy_nebius_config(config_path, http_port, authorization_server_address, allowed_proxy_host):
    with open(config_path, "w") as config:
        config.write(
            "\n".join(
                [
                    "generic:",
                    '  access_service_type: "nebius_v1"',
                    "  logging:",
                    "    stderr: true",
                    "  server:",
                    f"    http_port: {http_port}",
                    "  auth:",
                    "    tokens:",
                    '      access_service_type: "nebius_v1"',
                    "      secret_info:",
                    '        - name: "oidc-client-secret"',
                    '          secret: "test-secret"',
                    "      staff_api_user_token_info:",
                    '        name: "session-service-token"',
                    '        token: "test-service-token"',
                    "",
                    "oidc:",
                    '  client_id: "test-client"',
                    '  secret_name: "oidc-client-secret"',
                    '  session_service_token_name: "session-service-token"',
                    f'  authorization_server_address: "{authorization_server_address}"',
                    f'  allowed_proxy_hosts: ["{allowed_proxy_host}"]',
                ]
            )
        )


class OidcProxyService:
    def __init__(self, config_path, http_port):
        self.http_port = http_port
        self._daemon = Daemon(
            command=[
                mvp_oidc_proxy_bin(),
                "--config",
                config_path,
                "--http-port",
                str(http_port),
                "--stderr",
            ],
            cwd=yatest.common.output_path(),
            timeout=30,
            stdout_file=os.path.join(yatest.common.output_path(), "mvp_oidc_proxy.stdout"),
            stderr_file=os.path.join(yatest.common.output_path(), "mvp_oidc_proxy.stderr"),
            stderr_on_error_lines=100,
        )

    @property
    def endpoint(self):
        return f"http://localhost:{self.http_port}"

    def _is_ready(self):
        try:
            return requests.get(f"{self.endpoint}/ping", timeout=1).status_code == 200
        except requests.RequestException:
            return False

    def start(self):
        self._daemon.start()
        ready = wait_for(
            self._is_ready,
            timeout_seconds=30,
            step_seconds=0.5,
        )
        if not ready and self._daemon.is_alive():
            self._daemon.stop()
        assert ready, "mvp_oidc_proxy did not become ready on /ping"
        return self

    def stop(self):
        self._daemon.stop()


class OidcProxyEnv:
    def __init__(self, oidc_proxy):
        self.oidc_proxy = oidc_proxy

    @property
    def endpoint(self):
        return self.oidc_proxy.endpoint

    def get(self, path, **kwargs):
        kwargs.setdefault("timeout", 5)
        return requests.get(f"{self.endpoint}{path}", **kwargs)

    def post(self, path, **kwargs):
        kwargs.setdefault("timeout", 10)
        return requests.post(f"{self.endpoint}{path}", **kwargs)


class _BaseTestHandler(BaseHTTPRequestHandler):
    server_version = "TestHTTP/1.0"
    protocol_version = "HTTP/1.1"

    def log_message(self, format, *args):
        return


class _AuthServer(ThreadingHTTPServer):
    def __init__(self, server_address):
        super().__init__(server_address, _AuthHandler)
        self.token_requests = []
        self.exchange_requests = []


class _AuthHandler(_BaseTestHandler):
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


class THttpService:
    def __init__(self, server):
        self._server = server
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    @property
    def endpoint(self):
        host, port = self._server.server_address
        return f"http://{host}:{port}"

    @property
    def server(self):
        return self._server

    def start(self):
        self._thread.start()
        return self

    def stop(self):
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)


class OidcFullFlowEnv(OidcProxyEnv):
    def __init__(self, oidc_proxy, auth_service, cluster):
        super().__init__(oidc_proxy)
        self.auth_service = auth_service
        self.cluster = cluster


@contextmanager
def started_oidc_proxy_env():
    with PortManager() as port_manager:
        http_port = port_manager.get_port()
        config_path = os.path.join(yatest.common.output_path(), "oidc_proxy.yaml")
        write_oidc_proxy_config(config_path, http_port)
        oidc_proxy = OidcProxyService(config_path, http_port).start()
        try:
            yield OidcProxyEnv(oidc_proxy)
        finally:
            oidc_proxy.stop()


@contextmanager
def started_oidc_proxy_full_flow_env():
    with PortManager() as port_manager:
        auth_port = port_manager.get_port()
        http_port = port_manager.get_port()

        auth_service = THttpService(_AuthServer(("localhost", auth_port))).start()
        cluster = KiKiMR()
        cluster.start()

        config_path = os.path.join(yatest.common.output_path(), "oidc_proxy_full_flow.yaml")
        allowed_proxy_host = f"{cluster.nodes[1].host}:{cluster.nodes[1].mon_port}"
        write_oidc_proxy_nebius_config(
            config_path=config_path,
            http_port=http_port,
            authorization_server_address=auth_service.endpoint,
            allowed_proxy_host=allowed_proxy_host,
        )
        oidc_proxy = OidcProxyService(config_path, http_port).start()
        try:
            yield OidcFullFlowEnv(oidc_proxy, auth_service, cluster)
        finally:
            oidc_proxy.stop()
            auth_service.stop()
            cluster.stop()
