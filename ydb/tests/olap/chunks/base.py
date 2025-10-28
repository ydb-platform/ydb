import yatest.common
import os
import logging

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.olap.common.ydb_client import YdbClient

logger = logging.getLogger(__name__)


class ChunksTestBase(object):
    @classmethod
    def setup_class(cls):
        cls._setup_ydb()

    @classmethod
    def teardown_class(cls):
        cls.ydb_client.stop()
        cls.cluster.stop()

    @classmethod
    def _setup_ydb(cls):
        cls.ydbd_path = yatest.common.build_path(os.environ.get("YDB_DRIVER_BINARY"))
        cls.ydb_path = yatest.common.binary_path(os.environ.get("YDB_CLI_BINARY"))
        logger.info(yatest.common.execute([cls.ydbd_path, "-V"], wait=True).stdout.decode("utf-8"))
        config = KikimrConfigGenerator(
            column_shard_config={
                    "bulk_upsert_require_all_columns": "false",
                    "alter_object_enabled": "true",
                    "periodic_wakeup_activation_period_ms": 1000,
                    "combine_chunks_in_result": "false"
                }
        )
        cls.cluster = KiKiMR(config)
        cls.cluster.start()
        node = cls.cluster.nodes[1]
        cls.database = f"/{config.domain_name}"
        cls.endpoint = f"grpc://{node.host}:{node.port}"
        cls.ydb_client = YdbClient(database=cls.database, endpoint=cls.endpoint)
        cls.ydb_client.wait_connection()
