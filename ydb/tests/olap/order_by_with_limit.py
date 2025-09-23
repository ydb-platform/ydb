import logging
import os
import yatest.common
import ydb
import random

from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.olap.common.ydb_client import YdbClient

logger = logging.getLogger(__name__)


class TestOrderBy(object):
    test_name = "order_by"
    n = 200

    @classmethod
    def setup_class(cls):
        random.seed(0xBEDA)
        ydb_path = yatest.common.build_path(os.environ.get("YDB_DRIVER_BINARY"))
        logger.info(yatest.common.execute([ydb_path, "-V"], wait=True).stdout.decode("utf-8"))
        config = KikimrConfigGenerator(
            column_shard_config={
                "compaction_enabled": False,
                "deduplication_enabled": True,
                "reader_class_name": "SIMPLE",
            }
        )
        cls.cluster = KiKiMR(config)
        cls.cluster.start()
        node = cls.cluster.nodes[1]
        cls.ydb_client = YdbClient(database=f"/{config.domain_name}", endpoint=f"grpc://{node.host}:{node.port}")
        cls.ydb_client.wait_connection()

    def write_data(self, table: str):
        column_types = ydb.BulkUpsertColumns()
        column_types.add_column("id", ydb.PrimitiveType.Uint64)
        column_types.add_column("value", ydb.PrimitiveType.Uint64)

        # [2,  4,  6,  8, 10, ...]
        # [3,  6,  9, 12, 15, ...]
        # [4,  8, 12, 16, 20, ...]
        # [5, 10, 15, 20, 25, ...]
        # ...
        data = [[{"id": j, "value": j} for j in range(1, self.n) if j % i == 0] for i in range(2, self.n)]
        random.shuffle(data)

        for row in data:
            self.ydb_client.bulk_upsert(
                table,
                column_types,
                row,
            )

    def test(self):
        test_dir = f"{self.ydb_client.database}/{self.test_name}"
        table_path = f"{test_dir}/table"

        self.ydb_client.query(
            f"""
            CREATE TABLE `{table_path}` (
                id Uint64 NOT NULL,
                value Uint64 NOT NULL,
                PRIMARY KEY(id),
            )
            WITH (
                STORE = COLUMN,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1
            )
            """
        )

        self.write_data(table_path)

        for i in range(100):
            limit = random.randint(1, self.n)
            offset = random.randint(1, self.n)
            is_desc = random.randint(0, 1)
            is_le = random.randint(0, 1)
            order = ["asc", "desc"]
            condition = [">", "<"]
            data = list(range(2, self.n))
            if is_le:
                answer = [i for i in data if i < offset]
            else:
                answer = [i for i in data if i > offset]
            if is_desc:
                answer = answer[::-1]
            answer = answer[:limit]

            result_sets = self.ydb_client.query(
                f"""
                select id from `{table_path}`
                where value {condition[is_le]} {offset}
                order by id {order[is_desc]} limit {limit}
                """
            )

            keys = [row['id'] for result_set in result_sets for row in result_set.rows]

            assert keys == answer, keys
