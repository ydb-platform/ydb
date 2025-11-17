import logging
import os
import random
import yatest.common
import ydb
import pytest

from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.olap.common.ydb_client import YdbClient

logger = logging.getLogger(__name__)


# "Statement" is added to differ it from scenario/test_insert.py
class TestInsertStatement(object):
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
            },
            extra_feature_flags={
                "enable_columnshard_bool": True
            },
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

        result_sets = self.ydb_client.query(f"SELECT * FROM `{self.table_path}` ORDER BY id")

        # then
        rows = []
        for result_set in result_sets:
            rows.extend(result_set.rows)

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

    def test_bulk(self):
        # given
        self.create_table()

        # when
        # full
        self.ydb_client.query(f"INSERT INTO `{self.table_path}` (id, vn, vs) VALUES (0, 10, 'Zero'), (1, 11, 'One'), (2, 12, 'Two');")

        result_sets = self.ydb_client.query(f"SELECT * FROM `{self.table_path}` ORDER BY id")

        # then
        rows = []
        for result_set in result_sets:
            rows.extend(result_set.rows)

        assert len(rows) == 3
        assert rows[0] == {'id': 0, 'vn': 10, 'vs': 'Zero'}
        assert rows[1] == {'id': 1, 'vn': 11, 'vs': 'One'}
        assert rows[2] == {'id': 2, 'vn': 12, 'vs': 'Two'}

    def test_copy_full(self):
        # given
        self.create_table()
        self.write_data()
        t2 = self.get_table_path() + "_2f"
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
        t2 = self.get_table_path() + "_2p"
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
        result_sets = self.ydb_client.query(f"SELECT * FROM `{t2}` ORDER BY id")
        rows = []
        for result_set in result_sets:
            rows.extend(result_set.rows)

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
        except ydb.issues.PreconditionFailed as ex:
            assert "Conflict with existing key" in ex.message

        # then
        result_sets = self.ydb_client.query(f"SELECT * FROM `{self.table_path}`")

        rows = result_sets[0].rows
        assert len(rows) == 1
        assert rows[0] == {'id': 0, 'vn': 0, 'vs': 'Zero'}

    def test_insert_rollback(self):
        # given
        self.create_table()
        session = self.ydb_client.session_acquire()

        # when
        for i in range(5):
            tx = session.transaction().begin()
            tx.execute(f"INSERT INTO `{self.table_path}` (id, vn, vs) VALUES ({i}, {i}, '{i}');")
            tx.rollback()

        # then
        result_sets = self.ydb_client.query(f"SELECT count(*) AS cnt FROM `{self.table_path}`;")
        assert len(result_sets[0].rows) == 1
        assert result_sets[0].rows[0]['cnt'] == 0

        self.ydb_client.session_release(session)

    def test_incorrect(self):
        # given
        self.create_table()

        try:
            # when wrong table name
            self.ydb_client.query("INSERT INTO `wrongTable` (id, vn, vs) VALUES (0, 0, 'A');")
            # then
            assert False, 'Should Fail'
        except ydb.issues.SchemeError as ex:
            assert "Cannot find table" in ex.message

        try:
            # when wrong column name
            self.ydb_client.query(f"INSERT INTO `{self.table_path}` (id, wrongColumn, vs) VALUES (0, 0, 'A');")
            # then
            assert False, 'Should Fail'
        except ydb.issues.GenericError as ex:
            assert "No such column: wrongColumn" in ex.message

        try:
            # when wrong data type
            self.ydb_client.query(f"INSERT INTO `{self.table_path}` (id, vn, vs) VALUES (0, 'A', 0);")
            # then
            assert False, 'Should Fail'
        except ydb.issues.GenericError as ex:
            assert "Failed to convert type" in ex.message

    def test_out_of_range(self):
        rt = self.get_table_path() + "_r"
        self.ydb_client.query(
            f"""
            CREATE TABLE `{rt}` (
                id Uint64 NOT NULL,
                vu8 Uint8,
                PRIMARY KEY(id),
            )
            WITH (
                STORE = COLUMN
            )
            """
        )

        try:
            # when too big
            self.ydb_client.query(f"INSERT INTO `{rt}` (id, vu8) VALUES (0, 300);")
            # then
            assert False, 'Should Fail'
        except ydb.issues.GenericError as ex:
            assert "Failed to convert type" in ex.message

        try:
            # when too small
            self.ydb_client.query(f"INSERT INTO `{rt}` (id, vu8) VALUES (1, -1);")
            # then
            assert False, 'Should Fail'
        except ydb.issues.GenericError as ex:
            assert "Failed to convert type" in ex.message

    def test_bool_pk_error_type(self):
        table_path = self.get_table_path()
        self.ydb_client.query(
            f"""
            CREATE TABLE `{table_path}` (
                b Bool NOT NULL,
                PRIMARY KEY(b),
            )
            WITH (
                STORE = COLUMN
            )
            """
        )

        self.ydb_client.query(f"INSERT INTO `{table_path}` (b) VALUES (false);")
        try:
            self.ydb_client.query(f"INSERT INTO `{table_path}` (b) VALUES (false);")
            assert False, 'Should Fail'
        except ydb.issues.PreconditionFailed as ex:
            assert "Conflict with existing key" in ex.message
            # Check that error message doesn't contain "uint8" (bool stored as uint8 internally)
            assert "uint8" not in ex.message, f"Error message should not contain 'uint8', but it does: {ex.message}"

    @pytest.mark.parametrize("primitive_type", ["Uint64", "Int32", "Int16", "Uint8"])
    def test_primitive_pk_error_type(self, primitive_type):
        table_path = self.get_table_path()
        self.ydb_client.query(
            f"""
            CREATE TABLE `{table_path}` (
                type {primitive_type} NOT NULL,
                PRIMARY KEY(type),
            )
            WITH (
                STORE = COLUMN
            )
            """
        )

        self.ydb_client.query(f"INSERT INTO `{table_path}` (type) VALUES (239);")
        try:
            self.ydb_client.query(f"INSERT INTO `{table_path}` (type) VALUES (239);")
            assert False, 'Should Fail'
        except ydb.issues.PreconditionFailed as ex:
            assert "Conflict with existing key" in ex.message
            assert primitive_type.lower() in ex.message, f"Error message should contain '{primitive_type.lower()}', but it does: {ex.message}"
