import logging
import os
import random
import yatest.common
import ydb

from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.olap.common.ydb_client import YdbClient

logger = logging.getLogger(__name__)


class TestUpdate(object):
    test_name = "update"

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
                vn Int32,
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
        column_types.add_column("vn", ydb.PrimitiveType.Int32)
        column_types.add_column("vs", ydb.PrimitiveType.Utf8)

        data = []

        for i in range(100):
            data.append({"id": i, "vn": i, "vs": f"{i}"})

        self.ydb_client.bulk_upsert(
            self.table_path,
            column_types,
            data,
        )

    def test_update_single_row(self):
        # given
        self.create_table()
        self.write_data()

        # when
        self.ydb_client.query(f"UPDATE `{self.table_path}` SET vn = 100 + vn, vs = '103' WHERE id = 3;")

        # then
        result_sets = self.ydb_client.query(f"SELECT vn, vs FROM `{self.table_path}` WHERE id = 3;")

        assert len(result_sets[0].rows) == 1
        row = result_sets[0].rows[0]
        assert row['vn'] == 103
        assert row['vs'] == '103'

    def test_update_single_column(self):
        # given
        self.create_table()
        self.write_data()

        # when
        self.ydb_client.query(f"UPDATE `{self.table_path}` SET vs = 'ABC' WHERE id > 50;")

        # then
        result_sets = self.ydb_client.query(f"SELECT count(*) AS cnt FROM `{self.table_path}` WHERE vs = 'ABC';")
        assert len(result_sets[0].rows) == 1
        assert 49 == result_sets[0].rows[0]['cnt']

    def test_update_many_rows(self):
        # given
        self.create_table()
        self.write_data()

        # when
        self.ydb_client.query(f"UPDATE `{self.table_path}` SET vn = 101 + vn WHERE id < 50;")

        # then
        result_sets = self.ydb_client.query(f"SELECT count(*) AS cnt FROM `{self.table_path}` WHERE vn > 100")
        assert len(result_sets[0].rows) == 1
        assert 50 == result_sets[0].rows[0]['cnt']

    def test_partial_update(self):
        # given
        self.create_table()
        self.write_data()

        # when
        self.ydb_client.query(f"UPDATE `{self.table_path}` SET vs = 'one' WHERE id = 1;")

        # then
        result_sets = self.ydb_client.query(f"SELECT vn, vs FROM `{self.table_path}` WHERE id = 1;")

        assert len(result_sets[0].rows) == 1
        row = result_sets[0].rows[0]
        assert 1 == row['vn']
        assert 'one' == row['vs']

    def test_update_rollback(self):
        # given
        self.create_table()
        self.write_data()

        # when 0 (by_id)
        session = self.ydb_client.session_acquire()
        tx = session.transaction().begin()
        tx.execute(f"UPDATE `{self.table_path}` SET vs = 'whatever' WHERE id = 50;")
        tx.rollback()
        self.ydb_client.session_release(session)

        # then 0
        result_sets = self.ydb_client.query(f"SELECT vs FROM `{self.table_path}` WHERE id = 50;")
        assert len(result_sets[0].rows) == 1
        assert result_sets[0].rows[0]['vs'] == '50'

        # when 1 (all)
        session = self.ydb_client.session_acquire()
        tx = session.transaction().begin()
        tx.execute(f"UPDATE `{self.table_path}` SET vs = 'xyz';")
        tx.rollback()
        self.ydb_client.session_release(session)

        # then 1
        result_sets = self.ydb_client.query(f"SELECT count(*) AS cnt FROM `{self.table_path}` WHERE vs = 'xyz';")
        assert len(result_sets[0].rows) == 1
        assert result_sets[0].rows[0]['cnt'] == 0

    def test_incorrect(self):
        # given
        self.create_table()

        try:
            # when wrong table name
            self.ydb_client.query("UPDATE `wrongTable` SET vs = 'A' WHERE id = 1;")
            # then
            assert False, 'Should Fail'
        except ydb.issues.SchemeError as ex:
            assert "Cannot find table" in ex.message

        try:
            # when wrong column name
            self.ydb_client.query(f"UPDATE `{self.table_path}` SET wrongColumn = 'A' WHERE id = 1;")
            # then
            assert False, 'Should Fail'
        except ydb.issues.BadRequest as ex:
            assert "Column \\'wrongColumn\\' does not exist in table" in ex.message

        try:
            # when wrong data type
            self.ydb_client.query(f"UPDATE `{self.table_path}` SET vn = 'A' WHERE id = 1;")
            # then
            assert False, 'Should Fail'
        except ydb.issues.GenericError as ex:
            assert "Failed to convert \\'vn\\': String to Optional<Int32>" in ex.message

    def test_update_pk(self):
        # given
        self.create_table()
        self.write_data()

        try:
            # when
            self.ydb_client.query(f"UPDATE `{self.table_path}` SET id = 0;")
            # then
            assert False, 'Should Fail'
        except ydb.issues.GenericError as ex:
            assert "Cannot update primary key column: id" in ex.message
