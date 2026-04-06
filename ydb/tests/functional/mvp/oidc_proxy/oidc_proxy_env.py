import os
from contextlib import contextmanager

import yatest.common

from library.python.port_manager import PortManager

from ydb.tests.functional.mvp.common.http_env import BaseHttpEnv
from ydb.tests.functional.mvp.common.mock_nc_iam import started_mock_nc_iam
from ydb.tests.functional.mvp.common.mvp_service import MvpHttpService
from ydb.tests.library.harness.kikimr_runner import KiKiMR


def mvp_oidc_proxy_bin():
    return yatest.common.binary_path(os.getenv("OIDC_PROXY_BINARY"))


def started_mock_nc_iam_service():
    return started_mock_nc_iam()


def write_oidc_proxy_config(
    config_path,
    http_port,
    authorization_server_address,
    session_service_endpoint="localhost:8655",
    allowed_proxy_host=None,
):
    with open(config_path, "w") as config:
        config_lines = [
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
        ]
        if session_service_endpoint is not None:
            config_lines.append(f'  session_service_endpoint: "{session_service_endpoint}"')
        config_lines.extend([
            '  session_service_token_name: "session-service-token"',
            f'  authorization_server_address: "{authorization_server_address}"',
        ])
        if allowed_proxy_host is not None:
            config_lines.append(f'  allowed_proxy_hosts: ["{allowed_proxy_host}"]')
        config.write("\n".join(config_lines))


class OidcProxyService(MvpHttpService):
    def __init__(self, config_path, http_port):
        super().__init__(
            binary_path=mvp_oidc_proxy_bin(),
            config_path=config_path,
            http_port=http_port,
            service_name="mvp_oidc_proxy",
        )


class OidcProxyEnv(BaseHttpEnv):
    def __init__(self, oidc_proxy, auth_service=None):
        super().__init__(oidc_proxy)
        self.auth_service = auth_service


class OidcFullFlowEnv(OidcProxyEnv):
    def __init__(self, oidc_proxy, auth_service, cluster):
        super().__init__(oidc_proxy)
        self.auth_service = auth_service
        self.cluster = cluster


@contextmanager
def started_oidc_proxy_env():
    with PortManager() as port_manager:
        http_port = port_manager.get_port()
        with started_mock_nc_iam_service() as auth_service:
            config_path = os.path.join(yatest.common.output_path(), "oidc_proxy.yaml")
            write_oidc_proxy_config(config_path, http_port, auth_service.endpoint)
            oidc_proxy = OidcProxyService(config_path, http_port).start()
            try:
                yield OidcProxyEnv(oidc_proxy, auth_service=auth_service)
            finally:
                oidc_proxy.stop()


@contextmanager
def started_oidc_proxy_full_flow_env():
    with PortManager() as port_manager:
        http_port = port_manager.get_port()

        with started_mock_nc_iam_service() as auth_service:
            cluster = KiKiMR()
            cluster.start()
            try:
                config_path = os.path.join(yatest.common.output_path(), "oidc_proxy_full_flow.yaml")
                allowed_proxy_host = f"{cluster.nodes[1].host}:{cluster.nodes[1].mon_port}"
                write_oidc_proxy_config(
                    config_path=config_path,
                    http_port=http_port,
                    authorization_server_address=auth_service.endpoint,
                    allowed_proxy_host=allowed_proxy_host,
                    session_service_endpoint=None,
                )
                oidc_proxy = OidcProxyService(config_path, http_port).start()
                try:
                    yield OidcFullFlowEnv(oidc_proxy, auth_service, cluster)
                finally:
                    oidc_proxy.stop()
            finally:
                cluster.stop()
