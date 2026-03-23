import os
from contextlib import contextmanager

import requests
import yatest.common

from library.python.port_manager import PortManager

from ydb.tests.library.common.wait_for import wait_for
from ydb.tests.library.harness.daemon import Daemon


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
        return requests.get(f"{self.endpoint}{path}", timeout=5, **kwargs)


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
