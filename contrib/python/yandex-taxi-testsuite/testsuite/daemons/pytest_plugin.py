import contextlib
import inspect
import itertools
import signal
import subprocess
import typing
import warnings
from collections.abc import AsyncGenerator, Callable, Sequence
from typing import Any, AsyncContextManager

import aiohttp
import pytest

from testsuite import types
from testsuite._internal import fixture_types

from . import service_client, service_daemon
from .classes import DaemonInstance
from .spawn import __tracebackhide__  # noqa: F401

SHUTDOWN_SIGNALS = {
    'SIGINT': signal.SIGINT,
    'SIGKILL': signal.SIGKILL,
    'SIGQUIT': signal.SIGQUIT,
    'SIGTERM': signal.SIGTERM,
}


class _DaemonScope:
    def __init__(
        self,
        name: str,
        spawn: Callable,
        *,
        multiple: bool = False,
    ) -> None:
        self.name = name
        self._spawn = spawn
        self.multiple = multiple

    async def spawn(self) -> 'DaemonInstance':
        manager = self._spawn()
        # For backward compatibility with older spawners
        if inspect.iscoroutine(manager):
            warnings.warn(
                f'Please rewrite your spawner into async context manager {self._spawn}',
                PendingDeprecationWarning,
            )
            manager = await manager
        process = await manager.__aenter__()
        return DaemonInstance(manager, process)


class _DaemonStore:
    _cells: dict[str, tuple[_DaemonScope, DaemonInstance]]

    def __init__(self) -> None:
        self._cells = {}

    async def aclose(self, skip_multiple=False) -> None:
        cells_left = {}
        for name, (scope, daemon) in self._cells.items():
            if skip_multiple and scope.multiple:
                cells_left[name] = scope, daemon
            else:
                await self._close_daemon(daemon)
        self._cells = cells_left

    @contextlib.asynccontextmanager
    async def scope(
        self,
        name,
        spawn,
        *,
        multiple: bool = False,
    ) -> AsyncGenerator[_DaemonScope, None]:
        """
        Creates new scope evicting previous daemon.

        :param name: scope identifier.
        :param spawn: spawner instance.
        :param multiple: do not fail when this scope is requested with others.
        """
        scope = _DaemonScope(name, spawn, multiple=multiple)
        try:
            yield scope
        finally:
            await self._cleanup_cell(name)

    async def request(self, scope: _DaemonScope) -> DaemonInstance:
        if scope.name in self._cells:
            _, daemon = self._cells[scope.name]
            if daemon.process is None:
                return daemon
            if daemon.process.poll() is None:
                return daemon
        await self.aclose(skip_multiple=True)
        daemon = await scope.spawn()
        self._cells[scope.name] = scope, daemon
        return daemon

    def has_running_daemons(self) -> bool:
        for _, daemon in self._cells.values():
            if daemon.process and daemon.process.poll() is None:
                return True
        return False

    async def _cleanup_cell(self, name):
        if name not in self._cells:
            return
        scope, daemon = self._cells.pop(name)
        await self._close_daemon(daemon)

    async def _close_daemon(self, daemon: DaemonInstance):
        await daemon.aclose()


class EnsureDaemonStartedFixture(typing.Protocol):
    """Fixture that starts requested service."""

    async def __call__(self, scope: _DaemonScope) -> DaemonInstance: ...


class ServiceSpawnerFactory(typing.Protocol):
    def __call__(
        self,
        args: Sequence[str],
        *,
        base_command: Sequence[str] | None = None,
        env: dict[str, str] | None = None,
        poll_retries: int = service_daemon.POLL_RETRIES,
        ping_url: str | None = None,
        ping_request_timeout: float = service_daemon.PING_REQUEST_TIMEOUT,
        ping_response_codes: tuple[int] = service_daemon.PING_RESPONSE_CODES,
        health_check: service_daemon.HealthCheckType | None = None,
        subprocess_spawner: Callable[..., subprocess.Popen] | None = None,
        subprocess_options: dict[str, Any] | None = None,
        setup_service: Callable[[subprocess.Popen], None] | None = None,
        shutdown_signal: int | None = None,
        stdout_handler=None,
        stderr_handler=None,
    ):
        """Creates service spawner asynccontextmanager factory.

        :param args: command arguments
        :param base_command: Arguments to be prepended to ``args``.
        :param env: Environment variables dictionary.
        :param poll_retries: Number of tries for service health check
        :param ping_url: service health check url, service is considered up
            when 200 received.
        :param ping_request_timeout: Timeout for ping_url request
        :param ping_response_codes: HTTP resopnse codes tuple meaning that
            service is up and running.
        :param health_check: Async function to check service is running.
        :param subprocess_spawner: callable with `subprocess.Popen` interface.
        :param subprocess_options: Custom subprocess options.
        :param setup_service: Function to be called right after service
            is started.
        :param shutdown_signal: Signal used to stop running services.
        :returns: Return asynccontextmanager factory that might be used
                  within ``register_daemon_scope`` fixture.
        """


class CreateDaemonScope(typing.Protocol):
    """Create daemon scope for daemon with command to start."""

    def __call__(
        self,
        *,
        args: Sequence[str],
        ping_url: str | None = None,
        name: str | None = None,
        base_command: Sequence | None = None,
        env: dict[str, str] | None = None,
        poll_retries: int = service_daemon.POLL_RETRIES,
        ping_request_timeout: float = service_daemon.PING_REQUEST_TIMEOUT,
        ping_response_codes: tuple[int] = service_daemon.PING_RESPONSE_CODES,
        health_check: service_daemon.HealthCheckType | None = None,
        subprocess_options: dict[str, Any] | None = None,
        setup_service: Callable[[subprocess.Popen], None] | None = None,
        shutdown_signal: int | None = None,
        stdout_handler=None,
        stderr_handler=None,
        multiple=True,
    ) -> AsyncContextManager[_DaemonScope]:
        """
        :param args: command arguments
        :param base_command: Arguments to be prepended to ``args``.
        :param env: Environment variables dictionary.
        :param poll_retries: Number of tries for service health check
        :param ping_url: service health check url, service is considered up
            when 200 received.
        :param ping_request_timeout: Timeout for ping_url request
        :param ping_response_codes: HTTP resopnse codes tuple meaning that
            service is up and running.
        :param health_check: Async function to check service is running.
        :param subprocess_options: Custom subprocess options.
        :param setup_service: Function to be called right after service
            is started.
        :param shutdown_signal: Signal used to stop running services.
        :param multiple: do not fail when this scope is requested with others.
        :returns: Returns internal daemon scope instance to be used with
            ``ensure_daemon_started`` fixture.
        """


class CreateServiceClientFixture(typing.Protocol):
    """Creates service client instance.

    Example:

    .. code-block:: python

        def my_client(create_service_client):
            return create_service_client('http://localhost:9999/')
    """

    def __call__(
        self,
        base_url: str,
        *,
        client_class=service_client.Client,
        **kwargs,
    ):
        """
        :param base_url: base url for http client
        :param client_class: client class to use
        :returns: ``client_class`` instance
        """


@pytest.fixture
def ensure_daemon_started(
    _global_daemon_store: _DaemonStore, _testsuite_suspend_capture, pytestconfig
) -> EnsureDaemonStartedFixture:
    requests = set()

    async def ensure_daemon_started(scope: _DaemonScope) -> DaemonInstance:
        if not scope.multiple:
            requests.add(scope.name)
        if len(requests) > 1:
            pytest.fail(f'Test requested multiple daemons: {requests!r}')

        if pytestconfig.option.service_wait:
            with _testsuite_suspend_capture():
                return await _global_daemon_store.request(scope)
        return await _global_daemon_store.request(scope)

    return ensure_daemon_started


@pytest.fixture(scope='session')
def service_spawner_factory(
    pytestconfig: Any,
    service_client_session_factory: Any,
    wait_service_started: Any,
) -> ServiceSpawnerFactory:
    def service_spawner_factory(
        args: Sequence[str],
        *,
        base_command: Sequence[str] | None = None,
        env: dict[str, str] | None = None,
        poll_retries: int = service_daemon.POLL_RETRIES,
        ping_url: str | None = None,
        ping_request_timeout: float = service_daemon.PING_REQUEST_TIMEOUT,
        ping_response_codes: tuple[int] = service_daemon.PING_RESPONSE_CODES,
        health_check: service_daemon.HealthCheckType | None = None,
        subprocess_spawner: Callable[..., subprocess.Popen] | None = None,
        subprocess_options: dict[str, Any] | None = None,
        setup_service: Callable[[subprocess.Popen], None] | None = None,
        shutdown_signal: int | None = None,
        stdout_handler=None,
        stderr_handler=None,
    ):
        shutdown_timeout = pytestconfig.option.service_shutdown_timeout
        if shutdown_signal is None:
            shutdown_signal = SHUTDOWN_SIGNALS[
                pytestconfig.option.service_shutdown_signal
            ]

        health_check = service_daemon.make_health_check(
            ping_url=ping_url,
            ping_request_timeout=ping_request_timeout,
            ping_response_codes=ping_response_codes,
            health_check=health_check,
        )

        command_args = _build_command_args(args, base_command)

        @contextlib.asynccontextmanager
        async def spawn():
            if pytestconfig.option.service_wait:
                manager = wait_service_started(
                    args=command_args,
                    health_check=health_check,
                )
            elif pytestconfig.option.service_disable:
                manager = service_daemon.start_dummy_process()
            else:
                manager = service_daemon.start(
                    args=command_args,
                    env=env,
                    shutdown_signal=shutdown_signal,
                    shutdown_timeout=shutdown_timeout,
                    poll_retries=poll_retries,
                    health_check=health_check,
                    session_factory=service_client_session_factory,
                    subprocess_options=subprocess_options,
                    setup_service=setup_service,
                    subprocess_spawner=subprocess_spawner,
                    stdout_handler=stdout_handler,
                    stderr_handler=stderr_handler,
                )
            async with manager as process:
                yield process

        return spawn

    return service_spawner_factory


@pytest.fixture(scope='session')
def service_spawner(service_spawner_factory):
    def service_spawner(*args, **kwargs):
        factory = service_spawner_factory(*args, **kwargs)
        warnings.warn(
            'service_spawner() fixture is deprecated, '
            'use service_spawner_factory()',
            PendingDeprecationWarning,
        )

        async def spawner():
            return factory()

        return spawner

    return service_spawner


@pytest.fixture(scope='session')
def create_daemon_scope(
    _global_daemon_store: _DaemonStore,
    service_spawner_factory: ServiceSpawnerFactory,
) -> CreateDaemonScope:
    """Create daemon scope for daemon with command to start."""

    def create_daemon_scope(
        *,
        args: Sequence[str],
        ping_url: str | None = None,
        name: str | None = None,
        base_command: Sequence | None = None,
        env: dict[str, str] | None = None,
        poll_retries: int = service_daemon.POLL_RETRIES,
        ping_request_timeout: float = service_daemon.PING_REQUEST_TIMEOUT,
        ping_response_codes: tuple[int] = service_daemon.PING_RESPONSE_CODES,
        health_check: service_daemon.HealthCheckType | None = None,
        subprocess_options: dict[str, Any] | None = None,
        setup_service: Callable[[subprocess.Popen], None] | None = None,
        shutdown_signal: int | None = None,
        stdout_handler=None,
        stderr_handler=None,
        multiple=True,
    ) -> AsyncContextManager[_DaemonScope]:
        """
        :param args: command arguments
        :param base_command: Arguments to be prepended to ``args``.
        :param env: Environment variables dictionary.
        :param poll_retries: Number of tries for service health check
        :param ping_url: service health check url, service is considered up
            when 200 received.
        :param ping_request_timeout: Timeout for ping_url request
        :param ping_response_codes: HTTP resopnse codes tuple meaning that
            service is up and running.
        :param health_check: Async function to check service is running.
        :param subprocess_options: Custom subprocess options.
        :param setup_service: Function to be called right after service
            is started.
        :param shutdown_signal: Signal used to stop running services.
        :param multiple: do not fail when this scope is requested with others.
        :returns: Returns internal daemon scope instance to be used with
            ``ensure_daemon_started`` fixture.
        """
        if name is None:
            name = ' '.join(args)
        return _global_daemon_store.scope(
            name=name,
            spawn=service_spawner_factory(
                args=args,
                base_command=base_command,
                env=env,
                poll_retries=poll_retries,
                ping_url=ping_url,
                ping_request_timeout=ping_request_timeout,
                ping_response_codes=ping_response_codes,
                health_check=health_check,
                subprocess_options=subprocess_options,
                setup_service=setup_service,
                shutdown_signal=shutdown_signal,
                stdout_handler=stdout_handler,
                stderr_handler=stderr_handler,
            ),
            multiple=multiple,
        )

    return create_daemon_scope


@pytest.fixture
def create_service_client(
    service_client_default_headers: dict[str, str],
    service_client_options: dict[str, Any],
) -> CreateServiceClientFixture:
    def create_service_client(
        base_url: str,
        *,
        client_class=service_client.Client,
        **kwargs,
    ):
        """
        :param base_url: base url for http client
        :param client_class: client class to use
        :returns: ``client_class`` instance
        """
        return client_class(
            base_url,
            headers=service_client_default_headers,
            **service_client_options,
            **kwargs,
        )

    return create_service_client


@pytest.fixture(scope='session')
def wait_service_started(pytestconfig, service_client_session_factory):
    reporter = pytestconfig.pluginmanager.getplugin('terminalreporter')

    @contextlib.asynccontextmanager
    async def waiter(*, args, health_check):
        await service_daemon.service_wait(
            args=args,
            reporter=reporter,
            health_check=health_check,
            session_factory=service_client_session_factory,
        )
        yield None

    return waiter


def pytest_addoption(parser):
    group = parser.getgroup('services')
    group.addoption(
        '--service-timeout',
        metavar='TIMEOUT',
        help=(
            'Service client timeout in seconds. 0 means no timeout. '
            'Default is %(default)s'
        ),
        default=120.0,
        type=float,
    )
    group.addoption(
        '--service-disable',
        action='store_true',
        help='Do not start service daemon from testsuite',
    )
    group.addoption(
        '--service-wait',
        action='store_true',
        help='Wait for service to start outside of testsuite itself, e.g. gdb',
    )
    group.addoption(
        '--service-shutdown-timeout',
        help='Service shutdown timeout in seconds. Default is %(default)s',
        default=120.0,
        type=float,
    )
    group.addoption(
        '--service-shutdown-signal',
        help='Service shutdown signal. Default is %(default)s',
        default='SIGINT',
        choices=sorted(SHUTDOWN_SIGNALS.keys()),
    )


@pytest.fixture(scope='session')
def register_daemon_scope(_global_daemon_store: _DaemonStore):
    """Context manager that registers service process session.

    Yields daemon scope instance.

    :param name: service name
    :spawn spawn: asynccontextmanager service factory
    """
    return _global_daemon_store.scope


@pytest.fixture(scope='session')
def service_client_session_factory() -> service_daemon.ClientSessionFactory:
    def make_session(**kwargs):
        return aiohttp.ClientSession(**kwargs)

    return make_session


@pytest.fixture
async def service_client_session(
    service_client_session_factory,
) -> types.AsyncYieldFixture[aiohttp.ClientSession]:
    async with service_client_session_factory() as session:
        yield session


@pytest.fixture
def service_client_default_headers() -> dict[str, str]:
    """Default service client headers.

    Fill free to override in your conftest.py
    """
    return {}


@pytest.fixture
def service_client_options(
    pytestconfig,
    service_client_session: aiohttp.ClientSession,
    mockserver: fixture_types.MockserverFixture,
) -> types.YieldFixture[dict[str, Any]]:
    """Returns service client options dictionary."""
    yield {
        'session': service_client_session,
        'timeout': pytestconfig.option.service_timeout or None,
        'span_id_header': mockserver.span_id_header,
    }


@pytest.fixture(scope='session')
async def _global_daemon_store():
    store = _DaemonStore()
    async with contextlib.aclosing(store):
        yield store


@pytest.fixture(scope='session')
def _testsuite_suspend_capture(pytestconfig):
    capmanager = pytestconfig.pluginmanager.getplugin('capturemanager')

    @contextlib.contextmanager
    def suspend():
        try:
            capmanager.suspend_global_capture()
            yield
        finally:
            capmanager.resume_global_capture()

    return suspend


def _build_command_args(
    args: Sequence,
    base_command: Sequence | None,
) -> tuple[str, ...]:
    return tuple(str(arg) for arg in itertools.chain(base_command or (), args))
