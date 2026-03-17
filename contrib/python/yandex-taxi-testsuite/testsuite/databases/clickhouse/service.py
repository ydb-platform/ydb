import pathlib
import typing

from testsuite.environment import service, utils

from . import classes

DEFAULT_CLICKHOUSE_TCP_PORT = 17123
DEFAULT_CLICKHOUSE_HTTP_PORT = 17124

SERVICE_SCRIPT_PATH = pathlib.Path(__file__).parent.joinpath(
    'scripts/service-clickhouse',
)


class ServiceSettings(typing.NamedTuple):
    tcp_port: int
    http_port: int

    def get_connection_info(self) -> classes.ConnectionInfo:
        return classes.ConnectionInfo(
            host='localhost',
            tcp_port=self.tcp_port,
            http_port=self.http_port,
        )


def create_clickhouse_service(
    service_name,
    working_dir,
    settings: ServiceSettings | None = None,
    env: dict[str, str] | None = None,
):
    if settings is None:
        settings = get_service_settings()
    return service.ScriptService(
        service_name=service_name,
        script_path=str(SERVICE_SCRIPT_PATH),
        working_dir=working_dir,
        environment={
            'CLICKHOUSE_TMPDIR': working_dir,
            'CLICKHOUSE_TCP_PORT': str(settings.tcp_port),
            'CLICKHOUSE_HTTP_PORT': str(settings.http_port),
            **(env or {}),
        },
        check_ports=[settings.tcp_port, settings.http_port],
        start_timeout=utils.getenv_float(
            key='TESTSUITE_CLICKHOUSE_SERVER_START_TIMEOUT',
            default=20.0,
        ),
    )


def get_service_settings():
    return ServiceSettings(
        utils.getenv_int(
            key='TESTSUITE_CLICKHOUSE_SERVER_TCP_PORT',
            default=DEFAULT_CLICKHOUSE_TCP_PORT,
        ),
        utils.getenv_int(
            key='TESTSUITE_CLICKHOUSE_SERVER_HTTP_PORT',
            default=DEFAULT_CLICKHOUSE_HTTP_PORT,
        ),
    )
