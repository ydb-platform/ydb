import logging
import os
import pathlib
import time
import typing

from testsuite import types
from testsuite.utils import traceback

from . import shell, utils

logger = logging.getLogger(__name__)

_TESTSUITE_LIB_UTILS = pathlib.Path(__file__).parent.joinpath(
    'scripts/utils.sh',
)
COMMAND_START = 'start'
COMMAND_STOP = 'stop'


class BaseError(Exception):
    pass


class ServiceFailedToStartError(BaseError):
    pass


__tracebackhide__ = traceback.hide(BaseError)


class ScriptService:
    def __init__(
        self,
        *,
        service_name: str,
        script_path: str,
        working_dir: str,
        check_host: str = 'localhost',
        check_ports: list[int],
        environment: dict[str, str] | None = None,
        prestart_hook: typing.Callable | None = None,
        start_timeout: float = 2.0,
    ) -> None:
        self._service_name = service_name
        self._script_path = script_path
        self._environment = environment
        self._check_host = check_host
        self._check_ports = check_ports
        self._prestart_hook = prestart_hook
        self._start_timeout = start_timeout
        self._started_mark = StartedMark(working_dir)

    def ensure_started(self, *, verbose: int) -> None:
        self._started_mark.delete()
        self.stop(verbose=0)
        if self._prestart_hook:
            self._prestart_hook()
        if verbose:
            logger.info('Starting %s service...', self._service_name)
        self._command(COMMAND_START, verbose)
        if not self._wait_for_ports():
            raise ServiceFailedToStartError(
                f'Service {self._service_name} failed to start within {self._start_timeout} seconds.'
            )
        if verbose:
            logger.info('Service %s started.', self._service_name)
        self._started_mark.create()

    def stop(self, *, verbose: int) -> None:
        self._started_mark.delete()
        if verbose:
            logger.info('Stopping %s services...', self._service_name)
        self._command(COMMAND_STOP, verbose)
        if verbose:
            logger.info('Service %s stopped.', self._service_name)

    def is_running(self) -> bool:
        if not self._started_mark.exists():
            return False
        return all(
            utils.test_tcp_connection(self._check_host, port)
            for port in self._check_ports
        )

    def _command(self, command: str, verbose: int) -> None:
        env = os.environ.copy()
        env['TESTSUITE_LIB_UTILS'] = str(_TESTSUITE_LIB_UTILS)
        if self._environment:
            env.update(self._environment)
        args = [self._script_path, command]
        shell.execute(
            args,
            env=env,
            verbose=verbose,
            command_alias=f'env/{self._service_name}/{command}',
        )

    def _wait_for_ports(self) -> bool:
        start_time = time.perf_counter()
        for port in self._check_ports:
            time_passed = time.perf_counter() - start_time
            if time_passed >= self._start_timeout:
                return False

            if not utils.wait_tcp_connection(
                host=self._check_host,
                port=port,
                timeout=self._start_timeout - time_passed,
            ):
                return False

        return True


class StartedMark:
    def __init__(self, working_dir: types.PathOrStr) -> None:
        self._path = pathlib.Path(working_dir) / '.started'

    def create(self) -> None:
        self._path.parent.mkdir(exist_ok=True, parents=True)
        self._path.write_text('')

    def delete(self) -> None:
        try:
            self._path.unlink()
        except FileNotFoundError:
            pass

    def exists(self) -> bool:
        return self._path.exists()
