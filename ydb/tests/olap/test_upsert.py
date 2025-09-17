import logging
import os
import yatest.common
import ydb

from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.olap.common.ydb_client import YdbClient

logger = logging.getLogger(__name__)


class TestUpsert(object):
    test_name = "upsert"

    @classmethod
    def setup_class(cls):
        ydb_path = yatest.common.build_path(os.environ.get("YDB_DRIVER_BINARY"))
        logger.info(yatest.common.execute([ydb_path, "-V"], wait=True).stdout.decode("utf-8"))
        config = KikimrConfigGenerator(
            column_shard_config={
                "compaction_enabled": True,
                "deduplication_enabled": True,
                "reader_class_name": "SIMPLE",
            }
        )
        cls.cluster = KiKiMR(config)
        cls.cluster.start()
        node = cls.cluster.nodes[1]
        cls.ydb_client = YdbClient(database=f"/{config.domain_name}", endpoint=f"grpc://{node.host}:{node.port}")
        cls.ydb_client.wait_connection()

        test_dir = f"{cls.ydb_client.database}/{cls.test_name}"
        cls.table_path = f"{test_dir}/table"

    def create_table(self):
        self.ydb_client.query(
            f"""
            CREATE TABLE `{self.table_path}` (
                id Uint64 NOT NULL,
                vn Uint64,
                vs Utf8,
                PRIMARY KEY(id),
            )
            WITH (
                STORE = COLUMN
            )
            """
        )

    def write_data(self):
        column_types = ydb.BulkUpsertColumns()
        column_types.add_column("id", ydb.PrimitiveType.Uint64)
        column_types.add_column("vn", ydb.PrimitiveType.Uint64)
        column_types.add_column("vs", ydb.PrimitiveType.Utf8)

        data = []

        for i in range(100):
            data.append({"id": i, "vn": i, "vs": f"{i}"})

        self.ydb_client.bulk_upsert(
            self.table_path,
            column_types,
            data,
        )

    def test_count(self):
        # given
        self.create_table()
        self.write_data()

        for i in range(20):
            self.ydb_client.query(f"UPSERT INTO `{self.table_path}` (id, vn, vs) VALUES ({i + 90}, {i}, '{i}');")

        # when
        result_sets = self.ydb_client.query(f"SELECT count(*) AS cnt FROM `{self.table_path}`")

        # then
        assert len(result_sets[0].rows) == 1
        row = result_sets[0].rows[0]
        assert 110 == row['cnt']

    # not replace
    def test_partial_update(self):
        # given
        self.create_table()
        self.write_data()

        self.ydb_client.query(f"UPSERT INTO `{self.table_path}` (id, vs) VALUES (1, 'one');")

        # when
        result_sets = self.ydb_client.query(f"SELECT vn, vs FROM `{self.table_path}` WHERE id = 1;")

        # then
        assert len(result_sets[0].rows) == 1
        row = result_sets[0].rows[0]
        assert 1 == row['vn']
        assert 'one' == row['vs']

    def test_insert_nulls(self):
        # given
        self.create_table()
        self.ydb_client.query(f"UPSERT INTO `{self.table_path}` (id) VALUES (1);")

        # when
        result_sets = self.ydb_client.query(f"SELECT * FROM `{self.table_path}` WHERE id = 1;")

        # then
        assert len(result_sets[0].rows) == 1
        row = result_sets[0].rows[0]
        assert None == row['vn']
        assert None == row['vs']

    def test_copy_full(self):
        # given
        self.create_table()
        self.write_data()
        t2 = f"{self.table_path}_2"
        self.ydb_client.query(
            f"""
            CREATE TABLE `{t2}` (
                id Uint64 NOT NULL,
                vn Uint64,
                vs Utf8,
                PRIMARY KEY(id),
            )
            WITH (
                STORE = COLUMN
            )
            """
        )
        self.ydb_client.query(f"UPSERT INTO `{t2}` SELECT * FROM `{self.table_path}`;")

        # when
        result_sets = self.ydb_client.query(f"SELECT * FROM `{t2}`")
        rows = []
        for result_set in result_sets:
            rows.extend(result_set.rows)
        rows.sort(key=lambda x: x.id)

        # then
        assert len(rows) == 100
        for i in range(100):
            row = rows[i]
            assert i == row['id']
            assert i == row['vn']
            assert f"{i}" == row['vs']

    def test_copy_partial(self):
        # given
        self.create_table()
        self.write_data()
        t2 = f"{self.table_path}_p"
        self.ydb_client.query(
            f"""
            CREATE TABLE `{t2}` (
                id Uint64 NOT NULL,
                vs Utf8,
                PRIMARY KEY(id),
            )
            WITH (
                STORE = COLUMN
            )
            """
        )
        self.ydb_client.query(f"UPSERT INTO `{t2}` SELECT id, vs FROM `{self.table_path}`;")

        # when
        result_sets = self.ydb_client.query(f"SELECT * FROM `{t2}`")
        rows = []
        for result_set in result_sets:
            rows.extend(result_set.rows)
        rows.sort(key=lambda x: x.id)

        # then
        assert len(rows) == 100
        for i in range(100):
            row = rows[i]
            assert i == row['id']
            assert f"{i}" == row['vs']
