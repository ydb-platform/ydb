import logging
import os
import random
import yatest.common
import ydb

from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.olap.common.ydb_client import YdbClient

logger = logging.getLogger(__name__)


class TestSelect(object):
    test_name = "select"

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

    def test_json_query(self):
        # given
        self.table_path = self.get_table_path() + "_js"
        self.ydb_client.query(
            f"""
            CREATE TABLE `{self.table_path}` (
                id Uint64 NOT NULL,
                vjs Json,
                PRIMARY KEY(id),
            )
            WITH (
                STORE = COLUMN
            )
            """
        )

        # when
        self.ydb_client.query(f'INSERT INTO `{self.table_path}` (id, vjs) VALUES (11, @@{{"vals": [{{"vs": "a", "vn": 1}}]}}@@);')
        self.ydb_client.query(f'INSERT INTO `{self.table_path}` (id, vjs) VALUES (22, @@{{"vals": [{{"vs": "b", "vn": 2}}]}}@@);')
        self.ydb_client.query(f'INSERT INTO `{self.table_path}` (id, vjs) VALUES (33, @@{{"vals": [{{"vs": "c", "vn": 3}}]}}@@);')

        # positive
        result_sets = self.ydb_client.query(f'SELECT JSON_EXISTS(vjs, "$.vals[*].vs") AS e FROM `{self.table_path}`;')
        assert len(result_sets) > 0
        row = result_sets[0].rows[0]
        assert row['e'] is True

        # negative
        result_sets = self.ydb_client.query(f'SELECT JSON_EXISTS(vjs, "$.vals[1].vs") AS e FROM `{self.table_path}`;')
        assert len(result_sets) > 0
        row = result_sets[0].rows[0]
        assert row['e'] is False

        # positive
        result_sets = self.ydb_client.query(f'SELECT id FROM `{self.table_path}` WHERE JSON_VALUE(vjs, "$.vals[0].vn" RETURNING Int32) = 2;')
        assert len(result_sets) > 0
        row = result_sets[0].rows[0]
        assert row['id'] == 22

        # negative
        result_sets = self.ydb_client.query(f'SELECT * FROM `{self.table_path}` WHERE JSON_VALUE(vjs, "$.vals[0].vs") LIKE "%x%";')
        assert len(result_sets) > 0
        assert len(result_sets[0].rows) == 0

        # positive
        result_sets = self.ydb_client.query(f'SELECT JSON_QUERY(vjs, "$.vals") AS json FROM `{self.table_path}` WHERE id = 11;')
        assert len(result_sets) > 0
        row = result_sets[0].rows[0]
        assert row['json'] == [{"vs": "a", "vn": 1}]

    def test_incorrect(self):
        # given
        self.create_table()

        try:
            # when wrong table name
            self.ydb_client.query("SELECT * FROM `wrongTable`;")
            # then
            assert False, 'Should Fail'
        except ydb.issues.SchemeError as ex:
            assert "Cannot find table" in ex.message

        try:
            # when wrong column name
            self.ydb_client.query(f"SELECT wrongColumn FROM `{self.table_path}`;")
            # then
            assert False, 'Should Fail'
        except ydb.issues.GenericError as ex:
            assert "Member not found: wrongColumn" in ex.message

        try:
            # when wrong data type
            self.ydb_client.query(f"SELECT vs + 1 FROM `{self.table_path}` WHERE id = 1;")
            # then
            assert False, 'Should Fail'
        except ydb.issues.GenericError as ex:
            assert "Cannot add type Optional<Utf8> and Int32" in ex.message
