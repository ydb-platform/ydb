import logging
import pytest
import random
from ydb.tests.olap.column_compression.common.base import ColumnTestBase
from ydb.tests.library.common.helpers import plain_or_under_sanitizer
from ydb.tests.olap.common.column_table_helper import ColumnTableHelper

logger = logging.getLogger(__name__)


class TestCompressionBase(ColumnTestBase):
    ''' Implements https://github.com/ydb-platform/ydb/issues/13626 '''

    @classmethod
    def setup_class(cls):
        super(TestCompressionBase, cls).setup_class()

    @classmethod
    def upsert_and_wait_portions(self, table: ColumnTableHelper, number_rows_for_insert: int, count_upsert: int):
        current_num_rows: int = table.get_row_count()
        for _ in range(count_upsert):
            self.ydb_client.query(
                """
                $row_count = %i;
                $prev_count = %i;
                $rows= ListMap(ListFromRange(0, $row_count), ($i) -> {
                    return <|
                        value: $i + $prev_count,
                        value1: $i %% 9,
                    |>;
                });
                UPSERT INTO `%s`
                SELECT * FROM AS_TABLE($rows);
                """
                % (number_rows_for_insert, current_num_rows, table.path)
            )
            current_num_rows += number_rows_for_insert
            logger.info(
                f"{current_num_rows} rows in {table.path}. portions: {table.get_portion_stat_by_tier()}, blobs: {table.get_blob_stat_by_tier()}"
            )
        assert table.get_row_count() == current_num_rows

        if not self.wait_for(
            lambda: len(table.get_portion_stat_by_tier()) != 0, plain_or_under_sanitizer(70, 140)
        ):
            raise Exception("not all portions have been updated")

        if not self.wait_for(
            lambda: table.get_portion_stat_by_tier()['__DEFAULT']['Rows'] == current_num_rows, plain_or_under_sanitizer(70, 140)
        ):
            raise Exception("not all portions have been updated")


class TestAlterColumnCompression(TestCompressionBase):
    class_name = "alter_column_compression"

    @classmethod
    def setup_class(cls):
        super(TestAlterColumnCompression, cls).setup_class()
        cls.single_upsert_rows_count = 10000
        cls.upsert_count = 10
        cls.test_name = "all_supported_compression"
        cls.test_dir = f"{cls.ydb_client.database}/{cls.class_name}/{cls.test_name}"

    COMPRESSION_CASES = [
        ("default",          ''),
        ("lz4_compression",  'algorithm=lz4'),
        ("zstd_compression", 'algorithm=zstd'),
    ] + [
        (f"zstd_{lvl}_compression", f'algorithm=zstd,level={lvl}')
        for lvl in range(2, 22, 3)
    ]

    def create_table_without_compression(self, suffix):
        self.table_path = f"{self.test_dir}/to_compress_with_{suffix}__{random.randint(1000, 9999)}"

        self.ydb_client.query(
            f"""
                CREATE TABLE `{self.table_path}` (
                    value Uint64 NOT NULL COMPRESSION(algorithm=off),
                    value1 Uint64 COMPRESSION(algorithm=off),
                    PRIMARY KEY(value),
                )
                WITH (
                    STORE = COLUMN,
                    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1,
                    PARTITION_COUNT = 1
                )
            """
        )
        logger.info(f"Table {self.table_path} created")

        table = ColumnTableHelper(self.ydb_client, self.table_path)
        self.upsert_and_wait_portions(table, self.single_upsert_rows_count, self.upsert_count)

        expected_raw = self.upsert_count * self.single_upsert_rows_count * 8
        self.volumes_without_compression = table.get_volumes_column("value1")

        volumes = table.get_volumes_column("value1")
        assert volumes[0] == expected_raw
        assert table.get_portion_stat_by_tier()['__DEFAULT']['Rows'] == expected_raw // 8

    def create_table_with_compression(self, suffix, compression_settings):
        self.table_path = f"{self.test_dir}/to_decompress_from_{suffix}_{random.randint(1000, 9999)}"

        self.ydb_client.query(
            f"""
                CREATE TABLE `{self.table_path}` (
                    value Uint64 NOT NULL COMPRESSION({compression_settings}),
                    value1 Uint64 COMPRESSION({compression_settings}),
                    PRIMARY KEY(value),
                )
                WITH (
                    STORE = COLUMN,
                    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1,
                    PARTITION_COUNT = 1
                )
            """
        )
        logger.info(f"Table {self.table_path} created")
        table = ColumnTableHelper(self.ydb_client, self.table_path)
        self.upsert_and_wait_portions(table, self.single_upsert_rows_count, self.upsert_count)

        expected_raw = self.upsert_count * self.single_upsert_rows_count * 8
        self.volumes_with_compression = table.get_volumes_column("value1")

        volumes = table.get_volumes_column("value1")
        assert volumes[0] == expected_raw
        assert table.get_portion_stat_by_tier()['__DEFAULT']['Rows'] == expected_raw // 8

    @pytest.mark.parametrize("suffix, compression_settings", COMPRESSION_CASES)
    def test_alter_to_compression(self, suffix, compression_settings):
        self.create_table_without_compression(suffix)

        self.ydb_client.query(
            f"""
                ALTER TABLE `{self.table_path}`
                    ALTER COLUMN `value` SET COMPRESSION({compression_settings}),
                    ALTER COLUMN `value1` SET COMPRESSION({compression_settings});
            """
        )
        logger.info(f"Table {self.table_path} altered")
        # update items for them to be written with compression
        self.ydb_client.query(f"UPDATE `{self.table_path}` SET value1 = value1 + 1;")

        table = ColumnTableHelper(self.ydb_client, self.table_path)
        assert self.wait_for(lambda: table.get_portion_count() == 1, 70)

        volumes = table.get_volumes_column("value1")
        koef = self.volumes_without_compression[1] / volumes[1]
        logging.info(
            f"compression in `{table.path}` {self.volumes_without_compression[1]} / {volumes[1]}: {koef}"
        )
        assert koef > 1

    @pytest.mark.parametrize("suffix, compression_settings", COMPRESSION_CASES)
    def test_alter_from_compression(self, suffix, compression_settings):
        self.create_table_with_compression(suffix, compression_settings)

        self.ydb_client.query(
            f"""
                ALTER TABLE `{self.table_path}`
                    ALTER COLUMN `value` SET COMPRESSION(algorithm=off),
                    ALTER COLUMN `value1` SET COMPRESSION(algorithm=off);
            """
        )
        logger.info(f"Table {self.table_path} altered")
        # update items for them to be rewritten with compression
        self.ydb_client.query(f"UPDATE `{self.table_path}` SET value1 = value1 + 1;")

        table = ColumnTableHelper(self.ydb_client, self.table_path)
        assert self.wait_for(lambda: table.get_portion_count() == 1, 70)

        volumes = table.get_volumes_column("value1")
        koef = volumes[1] / self.volumes_with_compression[1]
        logging.info(
            f"compression in `{table.path}` {volumes[1]} / {self.volumes_with_compression[1]}: {koef}"
        )
        assert koef > 1

    def test_alter_from_lz4_to_zstd_compression(self):
        self.create_table_with_compression("lz4_compression",  'algorithm=lz4')

        self.ydb_client.query(
            f"""
                ALTER TABLE `{self.table_path}`
                    ALTER COLUMN `value` SET COMPRESSION(algorithm=zstd, level=9),
                    ALTER COLUMN `value1` SET COMPRESSION(algorithm=zstd, level=9);
            """
        )
        logger.info(f"Table {self.table_path} altered")
        # update items for them to be rewritten
        self.ydb_client.query(f"UPDATE `{self.table_path}` SET value1 = value1 + 1;")

        table = ColumnTableHelper(self.ydb_client, self.table_path)
        assert self.wait_for(lambda: table.get_portion_count() == 1, 70)

        volumes = table.get_volumes_column("value1")
        koef = self.volumes_with_compression[1] / volumes[1]
        logging.info(
            f"compression in `{table.path}` {self.volumes_with_compression[1]} / {volumes[1]}: {koef}"
        )
        assert koef > 1

    def test_alter_from_zstd_to_lz4_compression(self):
        self.create_table_with_compression("zstd_7_compression",  'algorithm=zstd, level=7')

        self.ydb_client.query(
            f"""
                ALTER TABLE `{self.table_path}`
                    ALTER COLUMN `value` SET COMPRESSION(algorithm=lz4),
                    ALTER COLUMN `value1` SET COMPRESSION(algorithm=lz4);
            """
        )
        logger.info(f"Table {self.table_path} altered")
        # update items for them to be rewritten
        self.ydb_client.query(f"UPDATE `{self.table_path}` SET value1 = value1 + 1;")

        table = ColumnTableHelper(self.ydb_client, self.table_path)
        assert self.wait_for(lambda: table.get_portion_count() == 1, 70)

        volumes = table.get_volumes_column("value1")
        koef = volumes[1] / self.volumes_with_compression[1]
        logging.info(
            f"compression in `{table.path}` {volumes[1]} / {self.volumes_with_compression[1]}: {koef}"
        )
        assert koef > 1

    def test_alter_zstd_level_from_0_to_19_compression(self):
        self.create_table_with_compression("zstd_compression",  'algorithm=zstd, level=0')

        self.ydb_client.query(
            f"""
                ALTER TABLE `{self.table_path}`
                    ALTER COLUMN `value` SET COMPRESSION(algorithm=zstd, level=19),
                    ALTER COLUMN `value1` SET COMPRESSION(algorithm=zstd, level=19);
            """
        )
        logger.info(f"Table {self.table_path} altered")
        # update items for them to be rewritten
        self.ydb_client.query(f"UPDATE `{self.table_path}` SET value1 = value1 + 1;")

        table = ColumnTableHelper(self.ydb_client, self.table_path)
        assert self.wait_for(lambda: table.get_portion_count() == 1, 70)

        volumes = table.get_volumes_column("value1")
        koef = self.volumes_with_compression[1] / volumes[1]
        logging.info(
            f"compression in `{table.path}` {self.volumes_with_compression[1]} / {volumes[1]}: {koef}"
        )
        assert koef > 1
