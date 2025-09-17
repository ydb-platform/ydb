import logging
import os
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

        test_dir = f"{cls.ydb_client.database}/{cls.test_name}"
        cls.table_path = f"{test_dir}/table"

    def create_table(self):
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

    def test_delete_by_key(self):
        # given
        self.create_table()
        self.write_data()

        # when 0 (non-existent)
        self.ydb_client.query(f"DELETE FROM `{self.table_path}` WHERE id = 300;")

        # then 0
        result_sets = self.ydb_client.query(f"SELECT count(*) AS cnt FROM `{self.table_path}`")
        assert len(result_sets[0].rows) == 1
        assert 100 == result_sets[0].rows[0]['cnt']

        # when 1 (equal)
        self.ydb_client.query(f"DELETE FROM `{self.table_path}` WHERE id = 42;")

        # then 1
        result_sets = self.ydb_client.query(f"SELECT count(*) AS cnt FROM `{self.table_path}`")
        assert len(result_sets[0].rows) == 1
        assert 99 == result_sets[0].rows[0]['cnt']

        # when 2 (condition)
        self.ydb_client.query(f"DELETE FROM `{self.table_path}` WHERE id > 50;")

        # then 2
        result_sets = self.ydb_client.query(f"SELECT count(*) AS cnt FROM `{self.table_path}`")
        assert len(result_sets[0].rows) == 1
        assert 50 == result_sets[0].rows[0]['cnt']

    def test_delete_by_column(self):
        # given
        self.create_table()
        self.write_data()

        # when 0 (non-existent)
        self.ydb_client.query(f"DELETE FROM `{self.table_path}` WHERE vf > 300;")

        # then 0
        result_sets = self.ydb_client.query(f"SELECT count(*) AS cnt FROM `{self.table_path}`")
        assert len(result_sets[0].rows) == 1
        assert 100 == result_sets[0].rows[0]['cnt']

        # when 1 (equal)
        self.ydb_client.query(f"DELETE FROM `{self.table_path}` WHERE vf > 41.5 AND vf < 42.5;")

        # then 1
        result_sets = self.ydb_client.query(f"SELECT count(*) AS cnt FROM `{self.table_path}`")
        assert len(result_sets[0].rows) == 1
        assert 99 == result_sets[0].rows[0]['cnt']

        # when 2 (condition)
        self.ydb_client.query(f"DELETE FROM `{self.table_path}` WHERE vf > 50.5;")

        # then 2
        result_sets = self.ydb_client.query(f"SELECT count(*) AS cnt FROM `{self.table_path}`")
        assert len(result_sets[0].rows) == 1
        assert 50 == result_sets[0].rows[0]['cnt']

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
        assert 1 == result_sets[0].rows[0]['cnt']

        # when 1 (all)
        session = self.ydb_client.session_acquire()
        tx = session.transaction().begin()
        tx.execute(f"DELETE FROM `{self.table_path}`;")
        tx.rollback()
        self.ydb_client.session_release(session)

        # then 1
        result_sets = self.ydb_client.query(f"SELECT count(*) AS cnt FROM `{self.table_path}`;")
        assert len(result_sets[0].rows) == 1
        assert 100 == result_sets[0].rows[0]['cnt']
