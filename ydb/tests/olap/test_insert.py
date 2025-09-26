import logging
import os
import yatest.common
import ydb

from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.olap.common.ydb_client import YdbClient

logger = logging.getLogger(__name__)


class TestInsert(object):
    test_name = "insert"

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

    def test_plain(self):
        # given
        self.create_table()

        # when
        # full
        self.ydb_client.query(f"INSERT INTO `{self.table_path}` (id, vn, vs) VALUES (0, 0, 'Zero');")
        # partial
        self.ydb_client.query(f"INSERT INTO `{self.table_path}` (id, vs) VALUES (1, 'One');")
        # just id
        self.ydb_client.query(f"INSERT INTO `{self.table_path}` (id) VALUES (2);")

        result_sets = self.ydb_client.query(f"SELECT * FROM `{self.table_path}`")

        # then
        rows = []
        for result_set in result_sets:
            rows.extend(result_set.rows)
        rows.sort(key=lambda x: x.id)

        assert len(rows) == 3
        assert rows[0]['id'] == 0
        assert rows[0]['vn'] == 0
        assert rows[0]['vs'] == 'Zero'
        assert rows[1]['id'] == 1
        assert rows[1]['vn'] is None
        assert rows[1]['vs'] == 'One'
        assert rows[2]['id'] == 2
        assert rows[2]['vn'] is None
        assert rows[2]['vs'] is None

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

        # when
        self.ydb_client.query(f"INSERT INTO `{t2}` SELECT * FROM `{self.table_path}`;")

        # then
        result_sets = self.ydb_client.query(f"SELECT count(*) AS cnt FROM `{t2}`")

        assert len(result_sets[0].rows) == 1
        assert result_sets[0].rows[0]['cnt'] == 100

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

        # when
        self.ydb_client.query(f"INSERT INTO `{t2}` SELECT id, vs FROM `{self.table_path}`;")

        # then
        result_sets = self.ydb_client.query(f"SELECT * FROM `{t2}`")
        rows = []
        for result_set in result_sets:
            rows.extend(result_set.rows)
        rows.sort(key=lambda x: x.id)

        assert len(rows) == 100
        for i in range(100):
            row = rows[i]
            assert row['id'] == i
            assert row['vs'] == f"{i}"

    def test_duplicate(self):
        # given
        self.create_table()

        # when
        # should pass
        self.ydb_client.query(f"INSERT INTO `{self.table_path}` (id, vn, vs) VALUES (0, 0, 'Zero');")
        # should fail
        try:
            self.ydb_client.query(f"INSERT INTO `{self.table_path}` (id, vn, vs) VALUES (0, 1, 'One');")
            assert False, 'Should Fail'
        except ydb.issues.PreconditionFailed:
            pass

        # then
        result_sets = self.ydb_client.query(f"SELECT * FROM `{self.table_path}`")

        rows = result_sets[0].rows
        assert len(rows) == 1
        assert rows[0]['id'] == 0
        assert rows[0]['vn'] == 0
        assert rows[0]['vs'] == 'Zero'

    def test_incorrect(self):
        # given
        self.create_table()

        try:
        # when wrong table name
            self.ydb_client.query("INSERT INTO `wrongTable` (id, vn, vs) VALUES (0, 0, 'A');")
        # then
            assert False, 'Should Fail'
        except ydb.issues.SchemeError:
            pass

        try:
        # when wrong column name
            self.ydb_client.query(f"INSERT INTO `{self.table_path}` (id, wrongColumn, vs) VALUES (0, 0, 'A');")
        # then
            assert False, 'Should Fail'
        except ydb.issues.GenericError:
            pass

        try:
        # when wrong data type
            self.ydb_client.query(f"INSERT INTO `{self.table_path}` (id, vn, vs) VALUES (0, 'A', 0);")
        # then
            assert False, 'Should Fail'
        except ydb.issues.GenericError:
            pass
