import logging
import os
import yatest.common
import pytest
import ydb

from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.olap.common.ydb_client import YdbClient

logger = logging.getLogger(__name__)


class TestCompactionConfig(object):
    test_name = "compaction_config"

    @classmethod
    def setup_class(cls):
        pass

    def init(self, config):
        ydb_path = yatest.common.build_path(os.environ.get("YDB_DRIVER_BINARY"))
        logger.info(yatest.common.execute([ydb_path, "-V"], wait=True).stdout.decode("utf-8"))
        self.cluster = KiKiMR(config)
        self.cluster.start()
        node = self.cluster.nodes[1]
        self.ydb_client = YdbClient(database=f"/{config.domain_name}", endpoint=f"grpc://{node.host}:{node.port}")
        self.ydb_client.wait_connection()

    def check(self, name):
        test_dir = f"{self.ydb_client.database}/{self.test_name}"
        table_path = f"{test_dir}/name"
        statement = f"""
            CREATE TABLE `{table_path}` (
                id Uint64 NOT NULL,
                value Uint64 NOT NULL,
                PRIMARY KEY(id),
            )
            WITH (
                STORE = COLUMN
            )
            """
        self.ydb_client.session_pool.execute_with_retries(statement, retry_settings=ydb.RetrySettings(max_retries=0))
        return self.ydb_client.driver.table_client.session().create().describe_table(table_path)

    def test_lc_buckets(self):
        config = KikimrConfigGenerator(
            column_shard_config={
                "default_compaction_preset": "lc-buckets",
            })

        self.init(config)
        self.check("test_lc_buckets")

    def test_tiling(self):
        config = KikimrConfigGenerator(
            column_shard_config={
                "default_compaction_preset": "tiling",
            })

        self.init(config)
        self.check("test_tiling")

    def test_wrong_preset(self):
        config = KikimrConfigGenerator(
            column_shard_config={
                "default_compaction_preset": "ydb",
            })

        with pytest.raises(Exception, match=r"Socket closed|recvmsg:Connection reset by peer"):
            self.init(config)
            self.check("test_wrong_preset")

    def test_default(self):
        config = KikimrConfigGenerator()

        self.init(config)
        self.check("test_default")

    def test_tiling_constructor(self):
        config = KikimrConfigGenerator(
            column_shard_config={
                "default_compaction_constructor": {
                    "class_name" : "tiling",
                    "tiling" : {
                        "json" : "{}"
                    }
                },
            })

        self.init(config)
        self.check("test_tiling_constructor")

    def test_lc_buckets_constructor(self):
        config = KikimrConfigGenerator(
            column_shard_config={
                "default_compaction_constructor": {
                    "class_name" : "lc-buckets",
                    "node_portions_count_limit" : 6000000,
                    "weight_kff" : 1,
                    "lcbuckets" : {
                        "levels" : [
                            {
                                "class_name" : "Zero",
                                "zero_level": {
                                    "portions_count_limit": 4000000,
                                    "expected_blobs_size": 2000000,
                                    "portions_live_duration_seconds": 180
                                }
                            },
                            {
                                "class_name" : "Zero",
                                "zero_level": {
                                    "portions_count_limit": 2000000,
                                    "expected_blobs_size": 18000000
                                }
                            },
                            {
                                "class_name" : "Zero",
                                "zero_level": {
                                    "portions_count_limit": 1000000,
                                    "expected_blobs_size": 8000000
                                }
                            },
                        ]
                    }
                },
            })
        self.init(config)
        self.check("test_lc_buckets_constructor")

    def test_wrong_constructor(self):
        config = KikimrConfigGenerator(
            column_shard_config={
                "default_compaction_constructor": {
                    "class_name" : "ydb",
                },
            })

        with pytest.raises(Exception, match=r"Socket closed|recvmsg:Connection reset by peer"):
            self.init(config)
            self.check("test_wrong_constructor")

    def test_mix_constructor(self):
        config = KikimrConfigGenerator(
            column_shard_config={
                "default_compaction_constructor": {
                    "class_name" : "lc-buckets",
                    "tiling" : {
                        "json" : "{}"
                    }
                },
            })
        with pytest.raises(Exception, match=r"Socket closed|recvmsg:Connection reset by peer"):
            self.init(config)
            self.check("test_mix_constructor")

    def test_constructor_overrides_preset(self):
        config = KikimrConfigGenerator(
            column_shard_config={
                "default_compaction_preset": "tiling",
                "default_compaction_constructor": {
                    "class_name" : "tiling",
                    "tiling" : {
                        "json" : "{}"
                    }
                },
            })

        self.init(config)
        self.check("test_constructor_overrides_preset")

    def test_constructor_still_overrides_preset(self):
        config = KikimrConfigGenerator(
            column_shard_config={
                "default_compaction_constructor": {
                    "class_name" : "tiling",
                    "tiling" : {
                        "json" : "{}"
                    }
                },
                "default_compaction_preset": "tiling",
            })

        self.init(config)
        self.check("test_constructor_still_overrides_preset")
