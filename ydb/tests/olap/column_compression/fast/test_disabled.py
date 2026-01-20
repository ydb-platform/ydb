import logging
import os
import pytest
import random
import yatest.common
import ydb

from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.olap.common.ydb_client import YdbClient

logger = logging.getLogger(__name__)


class TestWithCompressionDisabled(object):
    test_name = "with_olap_compression_disabled"

    COMPRESSION_CASES = [
        ("empty_compression",  ''),
        ("lz4_compression",  'algorithm=lz4'),
        ("zstd_compression", 'algorithm=zstd'),
        ("zstd_0_compression", 'algorithm=zstd,level=0'),
        ("zstd_9_compression", 'algorithm=zstd,level=9')
    ]

    @classmethod
    def setup_class(cls):
        ydb_path = yatest.common.build_path(os.environ.get("YDB_DRIVER_BINARY"))
        logger.info(yatest.common.execute([ydb_path, "-V"], wait=True).stdout.decode("utf-8"))
        config = KikimrConfigGenerator(
            disabled_feature_flags=[
                "enable_olap_compression"
            ]
        )
        cls.cluster = KiKiMR(config)
        cls.cluster.start()
        node = cls.cluster.nodes[1]
        cls.ydb_client = YdbClient(database=f"/{config.domain_name}", endpoint=f"grpc://{node.host}:{node.port}")
        cls.ydb_client.wait_connection()

        cls.test_dir = f"{cls.ydb_client.database}/{cls.test_name}"

    def get_table_path(self):
        # avoid using same table in parallel tests
        return f"{self.test_dir}/table{random.randrange(99999)}"

    @pytest.mark.parametrize("suffix, compression_settings", COMPRESSION_CASES)
    def test_create(self, suffix, compression_settings):
        table_path = self.get_table_path() + "_create_" + suffix
        try:
            self.ydb_client.query(
                f"""
                CREATE TABLE `{table_path}` (
                    id Uint64 NOT NULL,
                    val Int64 COMPRESSION({compression_settings}),
                    PRIMARY KEY(id),
                )
                WITH (STORE = COLUMN);
                """
            )
            assert False, "Should Fail"
        except ydb.issues.SchemeError as ex:
            assert "Compression is disabled for OLAP tables" in ex.message
