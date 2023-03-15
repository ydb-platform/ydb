import pytest

from ydb.tests.oss.ydb_sdk_import import ydb
from ydb.tests.library.harness.kikimr_http_client import HiveClient

# XXX: setting of pytest_plugins should work if specified directly in test modules
# but somehow it does not
#
# for ydb_{cluster, database, ...} fixture family
pytest_plugins = 'ydb.tests.library.harness.ydb_fixtures'


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
