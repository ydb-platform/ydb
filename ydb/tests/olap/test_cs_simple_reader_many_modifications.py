import yatest.common
import os
import logging
import random

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.olap.common.ydb_client import YdbClient

logger = logging.getLogger(__name__)


class TestCsSimpleReaderManyModifications(object):
    test_name = "cs_simple_reader_many_modifications"

    @classmethod
    def setup_class(cls):
        cls._setup_ydb()

    @classmethod
    def teardown_class(cls):
        cls.ydb_client.stop()
        cls.cluster.stop()

    @classmethod
    def _setup_ydb(cls):
        ydb_path = yatest.common.build_path(os.environ.get("YDB_DRIVER_BINARY"))
        logger.info(yatest.common.execute([ydb_path, "-V"], wait=True).stdout.decode("utf-8"))
        config = KikimrConfigGenerator(
            column_shard_config={
                "compaction_enabled": False,
                "deduplication_enabled": True,
                "reader_class_name": "SIMPLE"
            }
        )
        cls.cluster = KiKiMR(config)
        cls.cluster.start()
        node = cls.cluster.nodes[1]
        cls.ydb_client = YdbClient(database=f"/{config.domain_name}", endpoint=f"grpc://{node.host}:{node.port}")
        cls.ydb_client.wait_connection()

    def _get_test_dir(self):
        return f"{self.ydb_client.database}/{self.test_name}"

    def _check_rows_num(self, table_path, expected_rows_num):
        count = self.ydb_client.query(f"SELECT COUNT(*) as cnt FROM `{table_path}`;")[0].rows[0]["cnt"]
        assert count == expected_rows_num

    def test_many_modifications(self):
        table_path = f"{self._get_test_dir()}/test_many_modifications"
        self.ydb_client.query(f"DROP TABLE IF EXISTS `{table_path}`")
        self.ydb_client.query(
            f"""
            CREATE TABLE `{table_path}` (
                id Int32 NOT NULL,
                value Int64,
                PRIMARY KEY(id),
            )
            WITH (
                STORE = COLUMN
            )
            """
        )

        rows_num = 10
        mods_num = 1
        # Generate a list of primary keys
        pks = list(range(rows_num))

        # Generate a series of modifications for each key
        modifications = {}
        for pk in pks:
            modifications[pk] = []
            for _ in range(mods_num):
                modifications[pk].append(random.randint(0, 1000000))

        # Insert the initial rows
        for pk in pks:
            initial_value = modifications[pk][0]
            self.ydb_client.query(f"INSERT INTO `{table_path}` (id, value) VALUES ({pk}, {initial_value});")

        self._check_rows_num(table_path, rows_num)

        # Apply the subsequent modifications using UPDATE
        for pk in pks:
            for value in modifications[pk][1:]:
                self.ydb_client.query(f"UPDATE `{table_path}` SET value = {value} WHERE id = {pk};")

        # Verify the final state
        self._check_rows_num(table_path, rows_num)

        expected_data = {}
        for pk in pks:
            expected_data[pk] = modifications[pk][-1]

        # Select rows one by one
        for pk in pks:
            value = self.ydb_client.query(f"SELECT id, value FROM `{table_path}` where id = {pk};")[0].rows[0]["value"]
            assert value == expected_data[pk]

        rows = self.ydb_client.query(f"SELECT id, value FROM `{table_path}`;")[0].rows
        assert len(rows) == rows_num
        actual_data = {row['id']: row['value'] for row in rows}

        assert actual_data == expected_data
