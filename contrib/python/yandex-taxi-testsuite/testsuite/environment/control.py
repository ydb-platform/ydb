import dataclasses
import getpass
import pathlib
import typing

from testsuite.utils import yaml_util

from . import service, utils

CONFIG_PATH = pathlib.Path('~/.config/yasuite/env.yaml')
DEFAULT_WORKER_ID = 'master'


class BaseError(Exception):
    """Base class for exceptions of this module."""


class AlreadyStarted(BaseError):
    pass


class ServiceUnknown(BaseError):
    pass


@dataclasses.dataclass(frozen=True)
class Config:
    env_dir: pathlib.Path
    worker_id: str
    reuse_services: bool
    verbose: int


class Environment:
    config: Config
    _services: dict[str, service.ScriptService]
    _services_start_order: list[str]
    _env: dict[str, str] | None
    _service_factories: dict[str, typing.Callable]

    def __init__(
        self,
        config: Config,
        env: dict[str, str] | None = None,
    ) -> None:
        self.config = config
        self._services = {}
        self._services_start_order = []
        self._env = env
        self._service_factories = {}

    def register_service(self, name: str, factory) -> None:
        self._service_factories[name] = factory

    def ensure_started(self, service_name: str, **kwargs) -> None:
        if service_name not in self._services:
            self.start_service(service_name, **kwargs)

    def start_service(self, service_name: str, **kwargs) -> None:
        if service_name in self._services:
            raise AlreadyStarted(f'Service {service_name} is already started')
        script_service = self._create_service(service_name, **kwargs)
        if not (self.config.reuse_services and script_service.is_running()):
            script_service.ensure_started(verbose=self.config.verbose)
        self._services_start_order.append(service_name)
        self._services[service_name] = script_service

    def stop_service(self, service_name: str) -> None:
        if service_name not in self._services:
            self._services[service_name] = self._create_service(service_name)
        if not self.config.reuse_services:
            self._services[service_name].stop(verbose=self.config.verbose)

    def close(self) -> None:
        while self._services_start_order:
            service_name = self._services_start_order.pop()
            self.stop_service(service_name)
            self._services.pop(service_name)

    def _create_service(
        self,
        service_name: str,
        **kwargs,
    ) -> service.ScriptService:
        if service_name not in self._service_factories:
            raise ServiceUnknown(f'Unknown service {service_name} requested')
        service_class = self._service_factories[service_name]
        return service_class(
            service_name=service_name,
            working_dir=self._get_working_dir_for(service_name),
            env=self._env,
            **kwargs,
        )

    def _get_working_dir_for(self, service_name: str) -> pathlib.Path:
        working_dir = self.config.env_dir.joinpath(
            'services',
            utils.DOCKERTEST_WORKER,
            service_name,
        )
        if self.config.worker_id != 'master':
            return working_dir.joinpath('_' + self.config.worker_id)
        return working_dir


class TestsuiteEnvironment(Environment):
    def __init__(self, config: Config) -> None:
        if config.worker_id == DEFAULT_WORKER_ID:
            worker_suffix = '_' + config.worker_id
        else:
            worker_suffix = ''
        super().__init__(
            config=config,
            env={'WORKER_SUFFIX': worker_suffix},
        )


def load_environment_config(
    *,
    env_dir: pathlib.Path | None = None,
    worker_id: str = DEFAULT_WORKER_ID,
    reuse_services: bool = False,
    verbose: int = 0,
) -> Config:
    base_config = _load_config()
    if env_dir is None:
        if 'direcotry' in base_config:
            env_dir = pathlib.Path(base_config['direcotry'])
        else:
            env_dir = pathlib.Path(f'/tmp/.yasuite-{getpass.getuser()}')
    return Config(
        env_dir=env_dir.expanduser().resolve(),
        worker_id=worker_id,
        reuse_services=reuse_services,
        verbose=verbose,
    )


def _load_config():
    config_path = CONFIG_PATH.expanduser()
    if config_path.exists():
        return yaml_util.load_file(config_path)
    return {}
