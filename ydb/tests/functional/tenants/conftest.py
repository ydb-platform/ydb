import faulthandler
import logging
import time

import pytest

from ydb.tests.oss.ydb_sdk_import import ydb
from ydb.tests.library.clients.kikimr_http_client import HiveClient

# XXX: setting of pytest_plugins should work if specified directly in test modules
# but somehow it does not
#
# for ydb_{cluster, database, ...} fixture family
pytest_plugins = ['ydb.tests.library.fixtures', 'ydb.tests.library.flavours']


@pytest.fixture(scope='module')
def robust_retries():
    return ydb.RetrySettings().with_fast_backoff(
        ydb.BackoffSettings(ceiling=10, slot_duration=0.05, uncertain_ratio=0.1)
    )


@pytest.fixture(scope='module')
def config_hive(ydb_cluster):
    def _config_hive(boot_per_node=1, boot_batch_size=5):
        hive_cli = HiveClient(ydb_cluster.nodes[1].host, ydb_cluster.nodes[1].mon_port)
        hive_cli.set_max_scheduled_tablets(boot_per_node)
        hive_cli.set_max_boot_batch_size(boot_batch_size)
    return _config_hive


# Diagnostic-only hooks: preserve all auth/admin logs and locate long waits.
_diagnostic_logger = logging.getLogger("auth_admin_diagnostics")


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_protocol(item, nextitem):
    faulthandler.dump_traceback_later(10, repeat=True, file=2)
    try:
        yield
    finally:
        faulthandler.cancel_dump_traceback_later()


@pytest.hookimpl(hookwrapper=True)
def pytest_fixture_setup(fixturedef, request):
    started = time.monotonic()
    _diagnostic_logger.warning("AUTH_DIAG fixture start: %s", fixturedef.argname)
    try:
        yield
    finally:
        _diagnostic_logger.warning(
            "AUTH_DIAG fixture end: %s, %.3f seconds",
            fixturedef.argname, time.monotonic() - started,
        )


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()
    _diagnostic_logger.warning(
        "AUTH_DIAG phase: %s, %s, %.3f seconds, %s",
        item.nodeid, report.when, report.duration, report.outcome,
    )


@pytest.fixture(autouse=True)
def publish_auth_admin_diagnostics():
    yield
    pytest.fail("Intentional diagnostic failure: publish auth/admin logs", pytrace=False)
