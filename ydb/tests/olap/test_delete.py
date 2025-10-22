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


class TestDelete(object):
    test_name = "delete"

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

    def create_table(self):
        # avoid using same table in parallel tests
        self.table_path = f"{self.test_dir}/table{random.randrange(99999)}"
        self.ydb_client.query(
            f"""
            CREATE TABLE `{self.table_path}` (
                id Uint64 NOT NULL,
                vf Float,
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
        column_types.add_column("vf", ydb.PrimitiveType.Float)
        column_types.add_column("vs", ydb.PrimitiveType.Utf8)

        data = []

        for i in range(100):
            data.append({"id": i, "vf": i, "vs": f"{i}"})

        self.ydb_client.bulk_upsert(
            self.table_path,
            column_types,
            data,
        )

    @pytest.mark.parametrize(
        "expr, left",
        [
            # none-existent
            ("id = 101", 100),
            ("vf > 300", 100),
            ("vs LIKE 'nope'", 100),
            ("False", 100),
            # one row
            ("id = 42", 99),
            ("vf < 0.9", 99),
            ("vs LIKE '3'", 99),
            # multiple rows
            ("id > 50", 51),
            ("vf > 50.5", 51),
            # complex expression
            ("id >= 30 AND vf < 70", 60),
            ("id < 10 OR vf >= 90", 80),
            # all
            ("True", 0)
        ]
    )
    def test_delete_where(self, expr, left):
        # given
        self.create_table()
        self.write_data()

        # when
        self.ydb_client.query(f"DELETE FROM `{self.table_path}` WHERE {expr};")

        # then
        result_sets = self.ydb_client.query(f"SELECT count(*) AS cnt FROM `{self.table_path}`")
        assert len(result_sets[0].rows) == 1
        assert result_sets[0].rows[0]['cnt'] == left

    def test_delete_rollback(self):
        # given
        self.create_table()
        self.write_data()

        # when 0 (by_id)
        session = self.ydb_client.session_acquire()
        tx = session.transaction().begin()
        tx.execute(f"DELETE FROM `{self.table_path}` WHERE id = 50;")
        tx.rollback()
        self.ydb_client.session_release(session)

        # then 0
        result_sets = self.ydb_client.query(f"SELECT count(*) AS cnt FROM `{self.table_path}` WHERE id = 50;")
        assert len(result_sets[0].rows) == 1
        assert result_sets[0].rows[0]['cnt'] == 1

        # when 1 (all)
        session = self.ydb_client.session_acquire()
        tx = session.transaction().begin()
        tx.execute(f"DELETE FROM `{self.table_path}`;")
        tx.rollback()
        self.ydb_client.session_release(session)

        # then 1
        result_sets = self.ydb_client.query(f"SELECT count(*) AS cnt FROM `{self.table_path}`;")
        assert len(result_sets[0].rows) == 1
        assert result_sets[0].rows[0]['cnt'] == 100

    def test_incorrect(self):
        # given
        self.create_table()

        try:
            # when wrong table name
            self.ydb_client.query("DELETE FROM `wrongTable`;")
            # then
            assert False, 'Should Fail'
        except ydb.issues.SchemeError as ex:
            assert "Cannot find table" in ex.message

        try:
            # when wrong column name
            self.ydb_client.query(f"DELETE FROM `{self.table_path}` WHERE wrongColumn = 0;")
            # then
            assert False, 'Should Fail'
        except ydb.issues.GenericError as ex:
            assert "Member not found: wrongColumn" in ex.message

        try:
            # when wrong data type
            self.ydb_client.query(f"DELETE FROM `{self.table_path}` WHERE vf = 'A';")
            # then
            assert False, 'Should Fail'
        except ydb.issues.GenericError as ex:
            assert "Type annotation" in ex.message
