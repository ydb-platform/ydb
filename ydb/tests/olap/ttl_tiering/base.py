import yatest.common
import os
import time
import logging

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.util import LogLevels
from ydb.tests.olap.common.s3_client import S3Mock, S3Client
from ydb.tests.olap.common.ydb_client import YdbClient

logger = logging.getLogger(__name__)


class TllTieringTestBase(object):
    @classmethod
    def setup_class(cls):
        cls._setup_ydb()
        cls._setup_s3()

    @classmethod
    def teardown_class(cls):
        cls.s3_mock.stop()
        cls.ydb_client.stop()
        cls.cluster.stop()

    @classmethod
    def _setup_ydb(cls):
        ydb_path = yatest.common.build_path(os.environ.get("YDB_DRIVER_BINARY"))
        logger.info(yatest.common.execute([ydb_path, "-V"], wait=True).stdout.decode("utf-8"))
        config = KikimrConfigGenerator(
            extra_feature_flags={
                "enable_external_data_sources": True,
                "enable_tiering_in_column_shard": True
            },
            column_shard_config={
                "lag_for_compaction_before_tierings_ms": 0,
                "compaction_actualization_lag_ms": 0,
                "optimizer_freshness_check_duration_ms": 0,
                "small_portion_detect_size_limit": 0,
                "max_read_staleness_ms": 5000,
                "alter_object_enabled": True,
            },
            additional_log_configs={
                "TX_COLUMNSHARD_TIERING": LogLevels.DEBUG,
                "TX_COLUMNSHARD_ACTUALIZATION": LogLevels.TRACE,
                "TX_COLUMNSHARD_BLOBS_TIER": LogLevels.DEBUG,
            },
        )
        cls.cluster = KiKiMR(config)
        cls.cluster.start()
        node = cls.cluster.nodes[1]
        cls.ydb_client = YdbClient(database=f"/{config.domain_name}", endpoint=f"grpc://{node.host}:{node.port}")
        cls.ydb_client.wait_connection()

    @classmethod
    def _setup_s3(cls):
        cls.s3_mock = S3Mock(yatest.common.binary_path(os.environ["MOTO_SERVER_PATH"]))
        cls.s3_mock.start()

        cls.s3_pid = cls.s3_mock.s3_pid
        cls.s3_client = S3Client(endpoint=cls.s3_mock.endpoint)

    @staticmethod
    def wait_for(condition_func, timeout_seconds):
        t0 = time.time()
        while time.time() - t0 < timeout_seconds:
            if condition_func():
                return True
            time.sleep(1)
        return False
