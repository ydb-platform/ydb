import os
import pathlib
import socket
import typing
import warnings

from testsuite.environment import service, utils

from . import genredis

DEFAULT_MASTER_PORTS = (16379, 16389)
DEFAULT_SENTINEL_PORT = 26379
DEFAULT_SLAVE_PORTS = (16380, 16390, 16381)
DEFAULT_CLUSTER_PORTS = (17380, 17381, 17382, 17383, 17384, 17385)
DEFAULT_CLUSTER_REPLICAS = 1
DEFAULT_STANDALONE_PORT = 7000

SERVICE_SCRIPT_PATH = pathlib.Path(__file__).parent.joinpath(
    'scripts/service-redis',
)
CLUSTER_SERVICE_SCRIPT_PATH = pathlib.Path(__file__).parent.joinpath(
    'scripts/service-cluster-redis',
)
STANDALONE_SERVICE_SCRIPT_PATH = pathlib.Path(__file__).parent.joinpath(
    'scripts/service-standalone-redis',
)


class BaseError(Exception):
    pass


class NotEnoughPorts(BaseError):
    pass


class ServiceSettings(typing.NamedTuple):
    host: str
    master_ports: tuple[int, ...]
    sentinel_port: int
    slave_ports: tuple[int, ...]

    def validate(self):
        if len(self.master_ports) != len(DEFAULT_MASTER_PORTS):
            raise NotEnoughPorts(
                f'Need exactly {len(DEFAULT_MASTER_PORTS)} masters!',
            )
        if len(self.slave_ports) != len(DEFAULT_SLAVE_PORTS):
            raise NotEnoughPorts(
                f'Need exactly {len(DEFAULT_SLAVE_PORTS)} slaves!',
            )


class ClusterServiceSettings(typing.NamedTuple):
    host: str
    cluster_ports: tuple[int, ...]
    cluster_replicas: int

    def validate(self):
        if len(self.cluster_ports) % (self.cluster_replicas + 1) != 0:
            raise NotEnoughPorts(
                f'Number of nodes does not match number of replicas ({self.cluster_replicas})',
            )
        min_masters = (self.cluster_replicas + 1) * 3
        if len(self.cluster_ports) < min_masters:
            raise NotEnoughPorts(
                f'Need at least {min_masters} cluster nodes!',
            )


class StandaloneServiceSettings(typing.NamedTuple):
    host: str
    port: int


def get_service_settings():
    return ServiceSettings(
        host=_get_hostname(),
        master_ports=utils.getenv_ints(
            key='TESTSUITE_REDIS_MASTER_PORTS',
            default=DEFAULT_MASTER_PORTS,
        ),
        sentinel_port=utils.getenv_int(
            key='TESTSUITE_REDIS_SENTINEL_PORT',
            default=DEFAULT_SENTINEL_PORT,
        ),
        slave_ports=utils.getenv_ints(
            key='TESTSUITE_REDIS_SLAVE_PORTS',
            default=DEFAULT_SLAVE_PORTS,
        ),
    )


def get_cluster_service_settings():
    return ClusterServiceSettings(
        host=_get_hostname(),
        cluster_ports=utils.getenv_ints(
            key='TESTSUITE_REDIS_CLUSTER_PORTS',
            default=DEFAULT_CLUSTER_PORTS,
        ),
        cluster_replicas=utils.getenv_int(
            key='TESTSUITE_REDIS_CLUSTER_REPLICAS',
            default=DEFAULT_CLUSTER_REPLICAS,
        ),
    )


def get_standalone_service_settings():
    return StandaloneServiceSettings(
        host=_get_hostname(),
        port=utils.getenv_int(
            key='TESTSUITE_REDIS_STANDALONE_PORT',
            default=DEFAULT_STANDALONE_PORT,
        ),
    )


def create_redis_service(
    service_name,
    working_dir,
    settings: ServiceSettings | None = None,
    env=None,
):
    if settings is None:
        settings = get_service_settings()
    configs_dir = pathlib.Path(working_dir).joinpath('configs')
    check_ports = [
        settings.sentinel_port,
        *settings.master_ports,
        *settings.slave_ports,
    ]

    def prestart_hook():
        configs_dir.mkdir(parents=True, exist_ok=True)
        settings.validate()
        genredis.generate_redis_configs(
            output_path=configs_dir,
            host=settings.host,
            master0_port=settings.master_ports[0],
            master1_port=settings.master_ports[1],
            slave0_port=settings.slave_ports[0],
            slave1_port=settings.slave_ports[1],
            slave2_port=settings.slave_ports[2],
            sentinel_port=settings.sentinel_port,
        )

    return service.ScriptService(
        service_name=service_name,
        script_path=str(SERVICE_SCRIPT_PATH),
        working_dir=working_dir,
        environment={
            'REDIS_TMPDIR': working_dir,
            'REDIS_CONFIGS_DIR': str(configs_dir),
            'REDIS_SENTINEL_PORT': str(settings.sentinel_port),
            'REDIS_SENTINEL_HOST': settings.host,
            **(env or {}),
        },
        check_host=settings.host,
        check_ports=check_ports,
        prestart_hook=prestart_hook,
    )


def create_cluster_redis_service(
    service_name,
    working_dir,
    settings: ClusterServiceSettings | None = None,
    env=None,
):
    if settings is None:
        settings = get_cluster_service_settings()
    configs_dir = pathlib.Path(working_dir).joinpath('configs')
    check_ports = [
        *settings.cluster_ports,
    ]

    def prestart_hook():
        configs_dir.mkdir(parents=True, exist_ok=True)
        settings.validate()
        genredis.generate_cluster_redis_configs(
            output_path=configs_dir,
            host=settings.host,
            cluster_ports=settings.cluster_ports,
        )

    return service.ScriptService(
        service_name=service_name,
        script_path=str(CLUSTER_SERVICE_SCRIPT_PATH),
        working_dir=working_dir,
        environment={
            'REDIS_TMPDIR': working_dir,
            'REDIS_CONFIGS_DIR': str(configs_dir),
            'REDIS_HOST': settings.host,
            'REDIS_CLUSTER_PORTS': ' '.join(
                [str(port) for port in settings.cluster_ports]
            ),
            'REDIS_CLUSTER_REPLICAS': str(settings.cluster_replicas),
            **(env or {}),
        },
        check_host=settings.host,
        check_ports=check_ports,
        prestart_hook=prestart_hook,
    )


def create_standalone_redis_service(
    service_name,
    working_dir,
    settings: StandaloneServiceSettings | None = None,
    env=None,
):
    if settings is None:
        settings = get_standalone_service_settings()
    configs_dir = pathlib.Path(working_dir).joinpath('configs')

    def prestart_hook():
        configs_dir.mkdir(parents=True, exist_ok=True)
        genredis.generate_standalone_redis_config(
            output_path=configs_dir,
            host=settings.host,
            port=settings.port,
        )

    return service.ScriptService(
        service_name=service_name,
        script_path=str(STANDALONE_SERVICE_SCRIPT_PATH),
        working_dir=working_dir,
        environment={
            'REDIS_CONFIGS_DIR': str(configs_dir),
            **(env or {}),
        },
        check_host=settings.host,
        check_ports=[settings.port],
        prestart_hook=prestart_hook,
    )


def _get_hostname():
    hostname = 'localhost'
    for var in ('TESTSUITE_REDIS_HOSTNAME', 'HOSTNAME'):
        if var in os.environ:
            hostname = os.environ[var]
            break
    return _resolve_hostname(hostname)


def _resolve_hostname(hostname: str) -> str:
    for family in socket.AF_INET6, socket.AF_INET:
        try:
            result = socket.getaddrinfo(
                hostname,
                None,
                family=family,
                type=socket.SOCK_STREAM,
            )
        except OSError:
            continue
        if result:
            return result[0][4][0]  # type: ignore[return-value]
    warnings.warn(f'Failed to resolve hostname {hostname}')
    return hostname
