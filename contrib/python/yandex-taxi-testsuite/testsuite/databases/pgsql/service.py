import pathlib
import typing

from testsuite.environment import service, utils

from . import connection

DEFAULT_PORT = 15433

PLUGIN_DIR = pathlib.Path(__file__).parent
CONFIGS_DIR = PLUGIN_DIR.joinpath('configs')
SCRIPTS_DIR = PLUGIN_DIR.joinpath('scripts')


class ServiceSettings(typing.NamedTuple):
    port: int

    def get_conninfo(self) -> connection.PgConnectionInfo:
        return connection.PgConnectionInfo(
            host='localhost',
            port=self.port,
            user='testsuite',
        )


def get_service_settings():
    return ServiceSettings(
        utils.getenv_int(
            key='TESTSUITE_POSTGRESQL_PORT',
            default=DEFAULT_PORT,
        ),
    )


def create_pgsql_service(
    service_name,
    working_dir,
    settings: ServiceSettings | None = None,
    env=None,
):
    if settings is None:
        settings = get_service_settings()

    return service.ScriptService(
        service_name=service_name,
        script_path=str(SCRIPTS_DIR.joinpath('service-postgresql')),
        working_dir=working_dir,
        environment={
            'POSTGRESQL_TMPDIR': working_dir,
            'POSTGRESQL_CONFIGS_DIR': str(CONFIGS_DIR),
            'POSTGRESQL_PORT': str(settings.port),
            **(env or {}),
        },
        check_ports=[settings.port],
    )
