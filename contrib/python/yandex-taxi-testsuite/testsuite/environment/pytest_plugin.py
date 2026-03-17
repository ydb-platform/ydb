import pathlib

import pytest

from . import control, utils


class Hookspec:
    def pytest_service_register(self, register_service):
        """Register testsuite environment service."""


class TestsuiteEnvironmentPlugin:
    _env: control.TestsuiteEnvironment | None

    def __init__(self):
        self._env = None

    def ensure_started(self, service_name, **kwargs):
        if self._env:
            self._env.ensure_started(service_name, **kwargs)

    def pytest_sessionstart(self, session):
        start_environment = session.config.option.start_environment
        if start_environment == 'no':
            return
        if hasattr(session.config, 'workerinput'):
            worker_id = session.config.workerinput['workerid']
        elif session.config.pluginmanager.getplugin('dsession'):
            return
        else:
            worker_id = control.DEFAULT_WORKER_ID

        config = control.load_environment_config(
            env_dir=session.config.option.env_dir,
            worker_id=worker_id,
            reuse_services=start_environment == 'auto',
            verbose=session.config.option.verbose,
        )
        self._env = control.TestsuiteEnvironment(config)

        def register_service(service_name, factory=None):
            def decorator(factory):
                self._env.register_service(service_name, factory)

            if factory is None:
                return decorator
            return decorator(factory)

        session.config.pluginmanager.hook.pytest_service_register(
            register_service=register_service,
        )

    def pytest_sessionfinish(self, session):
        if self._env:
            self._env.close()

    def pytest_addhooks(self, pluginmanager):
        pluginmanager.add_hookspecs(Hookspec)

    def pytest_report_header(self, config):
        headers = []
        if self._env:
            if self._env.config.reuse_services:
                kind = 'auto'
            else:
                kind = 'new'
            headers.append(
                f'testsuite env: {kind}, dir: {self._env.config.env_dir}',
            )
        else:
            headers.append('testsuite env: external')
        return headers


def pytest_addoption(parser):
    group = parser.getgroup('env', 'Testsuite environment')
    group.addoption(
        '--env-dir',
        default=None,
        type=pathlib.Path,
        help='Path environment data directry.',
    )
    group.addoption(
        '--start-environment',
        choices=['yes', 'no', 'auto'],
        default='yes',
    )
    group.addoption(
        '--no-env',
        action='store_const',
        const='no',
        dest='start_environment',
        help='Use existing environment, do not start new',
    )
    group.addoption(
        '--auto-env',
        action='store_const',
        const='auto',
        dest='start_environment',
        help='If present use existing environment, otherwise start new',
    )
    group.addoption(
        '--start-env',
        action='store_const',
        const='yes',
        dest='start_environment',
        help='Start new environment',
    )


def pytest_configure(config):
    utils.ensure_non_root_user()
    config.pluginmanager.register(
        TestsuiteEnvironmentPlugin(),
        'testsuite_environment',
    )


@pytest.fixture(scope='session')
def ensure_service_started(pytestconfig):
    env: TestsuiteEnvironmentPlugin
    env = pytestconfig.pluginmanager.get_plugin('testsuite_environment')
    return env.ensure_started
