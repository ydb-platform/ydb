import yatest.common
import os
import time
import logging
import json

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.olap.common.ydb_client import YdbClient


logger = logging.getLogger(__name__)


class ColumnTestBase(object):
    @classmethod
    def setup_class(cls):
        cls._setup_ydb()

    @classmethod
    def teardown_class(cls):
        cls.ydb_client.stop()
        cls.cluster.stop()

    @classmethod
    def _setup_ydb(cls):
        ydb_path = yatest.common.build_path(os.environ.get("YDB_DRIVER_BINARY"))
        logger.info(yatest.common.execute([ydb_path, "-V"], wait=True).stdout.decode("utf-8"))
        config = KikimrConfigGenerator(
            extra_feature_flags={"enable_olap_compression": True},
            column_shard_config={
                "alter_object_enabled": True,
                "lag_for_compaction_before_tierings_ms": 0,
                "compaction_actualization_lag_ms": 0,
                "optimizer_freshness_check_duration_ms": 0,
                "small_portion_detect_size_limit": 0,
                "default_compaction_constructor": {
                    "class_name" : "tiling",
                    "tiling" : {
                        "json" : json.dumps({
                            "max_levels": 1,
                            "max_accumulate_portion_size": 0,
                            })
                    }
                },
            },
        )
        cls.cluster = KiKiMR(config)
        cls.cluster.start()
        node = cls.cluster.nodes[1]
        cls.ydb_client = YdbClient(database=f"/{config.domain_name}", endpoint=f"grpc://{node.host}:{node.port}")
        cls.ydb_client.wait_connection()

    @staticmethod
    def wait_for(condition_func, timeout_seconds):
        t0 = time.time()
        while time.time() - t0 < timeout_seconds:
            if condition_func():
                return True
            time.sleep(1)
        return False
