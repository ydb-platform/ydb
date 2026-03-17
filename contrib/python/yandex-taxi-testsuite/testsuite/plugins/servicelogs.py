import asyncio
import io
import logging
import pathlib

import pytest

from testsuite._internal import logreader

logger = logging.getLogger(__name__)


class LogFileReporter(logreader.LogFile):
    def __init__(self, path: pathlib.Path, *, formatter_factory):
        super().__init__(path)
        self._formatter_factory = formatter_factory

    def make_report(self):
        formatter = self._formatter_factory()
        log = io.StringIO()
        for line in self.readlines(limit_position=True):
            line = line.rstrip(b'\r\n')
            line = formatter(line)
            if line is not None:
                log.write(line)
                log.write('\n')
        return log.getvalue()


class ServiceLogsPlugin:
    _live_logs = None

    def __init__(self, *, config):
        self._config = config
        self._logs = {}
        self._flushers = []

    def pytest_sessionstart(self, session):
        if _live_logs_enabled(self._config):
            self._live_logs = logreader.LiveLogHandler()
        else:
            self._live_logs = None

    def pytest_sessionfinish(self, session):
        if self._live_logs:
            self._live_logs.join()

    def pytest_runtest_setup(self, item):
        self._flushers.clear()
        self.update_position()

    @pytest.hookimpl(wrapper=True, tryfirst=True)
    def pytest_runtest_makereport(self, item, call):
        report = yield
        if report.failed and not self._live_logs:
            self._userver_report_attach(report)
        return report

    def register_flusher(self, func):
        loop = asyncio.get_running_loop()
        self._flushers.append((loop, func))

    def register_logfile(
        self, path: pathlib.Path, title: str, formatter_factory
    ):
        logger.info('Watching service log file: %s', path)
        self._logs[path, title] = LogFileReporter(
            path, formatter_factory=formatter_factory
        )
        if self._live_logs:
            self._live_logs.register_logfile(
                path, formatter=formatter_factory()
            )

    def update_position(self):
        for logfile in self._logs.values():
            logfile.update_position()

    def _userver_report_attach(self, report):
        self._run_flushers()
        for (_, title), logfile in self._logs.items():
            self._userver_report_attach_log(logfile, report, title)

    def _userver_report_attach_log(self, logfile, report, title):
        value = logfile.make_report()
        if value:
            report.sections.append((f'Captured {title} {report.when}', value))

    def _run_flushers(self):
        try:
            for loop, flusher in self._flushers:
                if not loop.is_closed():
                    loop.run_until_complete(flusher())
        except Exception:
            logger.exception('failed to run logging flushers:')


def pytest_addoption(parser) -> None:
    parser.addoption(
        '--service-livelogs-disable',
        action='store_true',
        help='Disable service live logs (enabled with -s)',
    )


def pytest_configure(config):
    config.pluginmanager.register(
        ServiceLogsPlugin(config=config),
        'service_logs_plugin',
    )


@pytest.fixture(scope='session')
def servicelogs_register_logfile(_servicelogs_logging_plugin):
    def register(path: pathlib.Path, *, title: str, formatter_factory):
        _servicelogs_logging_plugin.register_logfile(
            path, title, formatter_factory
        )

    return register


@pytest.fixture
def servicelogs_register_flusher(
    _servicelogs_logging_plugin: ServiceLogsPlugin,
):
    def register(func):
        _servicelogs_logging_plugin.register_flusher(func)

    return register


@pytest.fixture(scope='session')
def service_logs_update_position(
    _servicelogs_logging_plugin: ServiceLogsPlugin,
):
    def update_position():
        return _servicelogs_logging_plugin.update_position()

    return update_position


@pytest.fixture(scope='session')
def _servicelogs_logging_plugin(pytestconfig) -> ServiceLogsPlugin:
    return pytestconfig.pluginmanager.get_plugin('service_logs_plugin')


def _live_logs_enabled(config):
    if not config.option.service_livelogs_disable:
        return bool(
            config.option.capture == 'no'
            and config.option.showcapture in ('all', 'log'),
        )
    return False
