import logging
import os
import yatest.common
from datetime import datetime, timedelta
import ydb

from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.olap.common.ydb_client import YdbClient

logger = logging.getLogger(__name__)


class TestMixedCompression(object):
    ''' Implements https://github.com/ydb-platform/ydb/issues/13626 '''
    test_name = "mixed_compression"

    @classmethod
    def setup_class(cls):
        ydb_path = yatest.common.build_path(os.environ.get("YDB_DRIVER_BINARY"))
        logger.info(yatest.common.execute([ydb_path, "-V"], wait=True).stdout.decode("utf-8"))
        config = KikimrConfigGenerator(
            extra_feature_flags=[
                "enable_olap_compression"
            ]
        )
        cls.cluster = KiKiMR(config)
        cls.cluster.start()
        node = cls.cluster.nodes[1]
        cls.ydb_client = YdbClient(database=f"/{config.domain_name}", endpoint=f"grpc://{node.host}:{node.port}")
        cls.ydb_client.wait_connection()

        cls.test_dir = f"{cls.ydb_client.database}/{cls.test_name}"

    def test_create_with_mixed_compression(self):
        table_path = f"{self.test_dir}/create_mixed_table"

        self.ydb_client.query(
            f"""
                CREATE TABLE `{table_path}` (
                    key Uint64 NOT NULL COMPRESSION(algorithm=off),
                    vInt Int32 COMPRESSION(algorithm=lz4),
                    vStr Utf8 COMPRESSION(algorithm=zstd),
                    vFlt Float COMPRESSION(algorithm=zstd, level=5),
                    vTs Timestamp COMPRESSION(algorithm=zstd, level=22),
                    PRIMARY KEY(key),
                )
                WITH (
                    STORE = COLUMN
                )
            """
        )

        now = datetime.now()
        data = []
        for i in range(100):
            data.append({
                'key': i,
                'vInt': i,
                'vStr': str(i),
                'vFlt': i * 0.1 - 1,
                'vTs': (now + timedelta(seconds=i)),  # .isoformat(),
            })

        column_types = ydb.BulkUpsertColumns()
        column_types.add_column("key", ydb.PrimitiveType.Uint64)
        column_types.add_column("vInt", ydb.PrimitiveType.Int32)
        column_types.add_column("vStr", ydb.PrimitiveType.Utf8)
        column_types.add_column("vFlt", ydb.PrimitiveType.Float)
        column_types.add_column("vTs", ydb.PrimitiveType.Timestamp)

        self.ydb_client.bulk_upsert(table_path, column_types, data)

        result_sets = self.ydb_client.query(f"SELECT * FROM `{table_path}` WHERE key = 42")

        assert len(result_sets[0].rows) == 1
        row = result_sets[0].rows[0]
        assert row['key'] == 42
        assert row['vInt'] == 42
        assert row['vStr'] == '42'
        assert 3.19 < row['vFlt'] < 3.21
        assert (now + timedelta(seconds=41)) < row['vTs'] < (now + timedelta(seconds=43))
