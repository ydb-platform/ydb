import asyncio
import traceback

import pytest

OUTPUT_PREFIX = '[service-runner] '


class Hookspec:
    def pytest_servicetest_modifyitem(self, session, item):
        """Modify items for servicetest runner."""


class ServiceTestRunner:
    def __init__(self, config):
        self.config = config
        self.service_started = False

    @property
    def reporter(self):
        return self.config.pluginmanager.get_plugin('terminalreporter')

    # Filter out all but servicetest items
    @pytest.hookimpl(trylast=True)
    def pytest_collection_modifyitems(self, session, items):
        service_items = []
        for item in items:
            for marker in item.own_markers:
                if marker.name == 'servicetest':
                    service_items.append(item)
                    break
        if len(service_items) != 1:
            self.reporter.write_line(
                OUTPUT_PREFIX
                + (
                    f'You have to select only one servicetest, '
                    f'{len(service_items)} selected'
                ),
                red=True,
            )
            raise session.Failed

        for item in service_items:
            self.config.hook.pytest_servicetest_modifyitem(
                session=session,
                item=item,
            )
        items[:] = service_items

    def pytest_sessionstart(self, session):
        reporter = self.config.pluginmanager.get_plugin('terminalreporter')
        reporter.write_line(
            OUTPUT_PREFIX + 'Running in servicetest mode',
            yellow=True,
        )

    def pytest_runtestloop(self, session):
        item = session.items[0]
        item.config.hook.pytest_runtest_protocol(item=item, nextitem=None)
        if session.shouldfail:
            raise session.Failed(session.shouldfail)
        if session.shouldstop:
            raise session.Interrupted(session.shouldstop)
        return True

    @pytest.hookimpl(hookwrapper=True, tryfirst=True)
    def pytest_runtest_call(self, item):
        try:
            result = yield
            result.get_result()
        except Exception:
            self.reporter.write_line(
                OUTPUT_PREFIX + 'Failed to start service',
                red=True,
            )
            exc_info = traceback.format_exc()
            self.reporter.write_line(OUTPUT_PREFIX + exc_info, red=True)
            raise
        else:
            self.service_started = True


def pytest_addoption(parser):
    group = parser.getgroup('service runner', 'servicerunner')
    group.addoption(
        '--service-runner-mode',
        action='store_true',
        help='Run pytest in service runner mode',
    )


def pytest_configure(config):
    config.addinivalue_line(
        'markers',
        'servicetest: mark test as service entrypoint',
    )
    if config.option.service_runner_mode:
        runner = ServiceTestRunner(config)
        config.pluginmanager.register(runner, 'servicetest_runner')


def pytest_addhooks(pluginmanager):
    pluginmanager.add_hookspecs(Hookspec)


def pytest_servicetest_modifyitem(session, item):
    item.fixturenames.append('_servicetest_endless_sleep')


@pytest.fixture
async def _servicetest_endless_sleep(pytestconfig, _global_daemon_store):
    yield
    servicetest_runner = pytestconfig.pluginmanager.get_plugin(
        'servicetest_runner',
    )
    if not servicetest_runner.service_started:
        return
    servicetest_runner.reporter.write_line(
        OUTPUT_PREFIX + 'Service started, use C-c to terminate',
        yellow=True,
    )
    while _global_daemon_store.has_running_daemons():
        await asyncio.sleep(1)
