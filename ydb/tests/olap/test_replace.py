import logging
import os
import random
import yatest.common
import ydb

from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.olap.common.ydb_client import YdbClient

logger = logging.getLogger(__name__)


class TestReplace(object):
    test_name = "replace"

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

        cls.test_dir = f"{cls.ydb_client.database}/{cls.test_name}"

    def get_table_path(self):
        # avoid using same table in parallel tests
        return f"{self.test_dir}/table{random.randrange(99999)}"

    def create_table(self):
        self.table_path = self.get_table_path()
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

        # when
        for i in range(20):
            self.ydb_client.query(f"REPLACE INTO `{self.table_path}` (id, vn, vs) VALUES ({i + 90}, {i}, '{i}');")

        # then
        result_sets = self.ydb_client.query(f"SELECT count(*) AS cnt FROM `{self.table_path}`")

        assert len(result_sets[0].rows) == 1
        row = result_sets[0].rows[0]
        assert row['cnt'] == 110

    def test_partial_replace(self):
        # given
        self.create_table()
        self.write_data()

        # when
        self.ydb_client.query(f"REPLACE INTO `{self.table_path}` (id, vs) VALUES (1, 'one');")

        # then
        result_sets = self.ydb_client.query(f"SELECT vn, vs FROM `{self.table_path}` WHERE id = 1;")

        assert len(result_sets[0].rows) == 1
        row = result_sets[0].rows[0]
        assert row['vn'] is None
        assert row['vs'] == 'one'

    def test_replace_with_nulls(self):
        # given
        self.create_table()

        # when
        self.ydb_client.query(f"REPLACE INTO `{self.table_path}` (id) VALUES (1);")

        # then
        result_sets = self.ydb_client.query(f"SELECT * FROM `{self.table_path}` WHERE id = 1;")

        assert len(result_sets[0].rows) == 1
        row = result_sets[0].rows[0]
        assert row['vn'] is None
        assert row['vs'] is None

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
        self.ydb_client.query(f"REPLACE INTO `{t2}` SELECT * FROM `{self.table_path}`;")

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
            assert row['vn'] == i
            assert row['vs'] == f"{i}"

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
        self.ydb_client.query(f"REPLACE INTO `{t2}` SELECT id, vs FROM `{self.table_path}`;")

        # then
        result_sets = self.ydb_client.query(f"SELECT * FROM `{t2}`")
        rows = []
        for result_set in result_sets:
            rows.extend(result_set.rows)
        rows.sort(key=lambda x: x.id)

        assert len(rows) == 100
        for i in range(100):
            row = rows[i]
            assert i == row['id']
            assert f"{i}" == row['vs']

    def test_replace_total(self):
        # given
        self.create_table()
        self.write_data()

        # when
        self.ydb_client.query(f"REPLACE INTO `{self.table_path}` (id, vn, vs) SELECT id, vn + 100, vs FROM `{self.table_path}`;")

        # then
        result_sets = self.ydb_client.query(f"SELECT count(*) AS cnt FROM `{self.table_path}` WHERE vn < 100")

        assert len(result_sets[0].rows) == 1
        row = result_sets[0].rows[0]
        assert row['cnt'] == 0

    def test_replace_rollback(self):
        # given
        self.create_table()
        self.write_data()

        # when 0 (full)
        session = self.ydb_client.session_acquire()
        tx = session.transaction().begin()
        tx.execute(f"REPLACE INTO `{self.table_path}` (id, vn, vs) VALUES (9, 101, 'A');")
        tx.rollback()
        self.ydb_client.session_release(session)

        # then 0
        result_sets = self.ydb_client.query(f"SELECT vn, vs FROM `{self.table_path}` WHERE id = 9;")
        assert len(result_sets[0].rows) == 1
        assert result_sets[0].rows[0]['vn'] == 9
        assert result_sets[0].rows[0]['vs'] == '9'

        # when 1 (with nulls)
        session = self.ydb_client.session_acquire()
        tx = session.transaction().begin()
        tx.execute(f"REPLACE INTO `{self.table_path}` (id) VALUES (9);")
        tx.rollback()
        self.ydb_client.session_release(session)

        # then 1
        result_sets = self.ydb_client.query(f"SELECT count(*) AS cnt FROM `{self.table_path}` WHERE vs IS NULL;")
        assert len(result_sets[0].rows) == 1
        assert result_sets[0].rows[0]['cnt'] == 0

    def test_incorrect(self):
        # given
        self.create_table()

        try:
            # when wrong table name
            self.ydb_client.query("REPLACE INTO `wrongTable` (id, vn, vs) VALUES (0, 0, 'A');")
            # then
            assert False, 'Should Fail'
        except ydb.issues.SchemeError as ex:
            assert "Cannot find table" in ex.message

        try:
            # when wrong column name
            self.ydb_client.query(f"REPLACE INTO `{self.table_path}` (id, wrongColumn, vs) VALUES (0, 0, 'A');")
            # then
            assert False, 'Should Fail'
        except ydb.issues.GenericError as ex:
            assert "No such column: wrongColumn" in ex.message

        try:
            # when wrong data type
            self.ydb_client.query(f"REPLACE INTO `{self.table_path}` (id, vn, vs) VALUES (0, 'A', 0);")
            # then
            assert False, 'Should Fail'
        except ydb.issues.GenericError as ex:
            assert "Failed to convert type" in ex.message
