import logging
from .base import ColumnTestBase
from ydb.tests.library.common.helpers import plain_or_under_sanitizer
from ydb.tests.olap.common.column_table_helper import ColumnTableHelper
import pytest

logger = logging.getLogger(__name__)


class TestCompressionBase(ColumnTestBase):

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
                        value1: $i + $prev_count,
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

    @classmethod
    def add_family_in_create(self, name: str, settings: str):
        return f"FAMILY {name} ({settings})"


class TestCreateWithColumnCompression(TestCompressionBase):
    class_name = "crate_with_column_compression"

    @classmethod
    def setup_class(cls):
        super(TestCreateWithColumnCompression, cls).setup_class()
        cls.single_upsert_rows_count: int = 10**4
        cls.upsert_count: int = 10
        cls.volumes_without_compression: tuple[int, int]
        cls.test_name: str = "all_supported_compression"
        cls.test_dir: str = f"{cls.ydb_client.database}/{cls.class_name}/{cls.test_name}"
        cls.create_table_without_compression()

    COMPRESSION_CASES = [
        ("lz4_compression",  'algorithm=lz4'),
        ("zstd_compression", 'algorithm=zstd'),
    ] + [
        (f"zstd_{lvl}_compression", f'algorithm=zstd,level={lvl}')
        for lvl in range(2, 22)
    ]

    @classmethod
    def create_table_without_compression(cls):
        table_path: str = f"{cls.test_dir}/off_compression"

        cls.ydb_client.query(
            f"""
                CREATE TABLE `{table_path}` (
                    value Uint64 NOT NULL COMPRESSION(algorithm=off),
                    value1 Uint64 COMPRESSION(algorithm=off),
                    PRIMARY KEY(value),
                )
                WITH (STORE = COLUMN)
                """
        )
        logger.info(f"Table {table_path} created")
        table = ColumnTableHelper(cls.ydb_client, table_path)
        cls.upsert_and_wait_portions(table, cls.single_upsert_rows_count, cls.upsert_count)

        expected_raw = cls.upsert_count * cls.single_upsert_rows_count * 8
        cls.volumes_without_compression: tuple[int, int] = table.get_volumes_column("value")

        volumes = table.get_volumes_column("value")
        assert volumes[0] == expected_raw
        assert table.get_portion_stat_by_tier()['__DEFAULT']['Rows'] == expected_raw // 8

    @pytest.mark.parametrize("suffix, compression_settings", COMPRESSION_CASES)
    def test_all_supported_compression(self, suffix: str, compression_settings: str):
        ''' Implements https://github.com/ydb-platform/ydb/issues/13640 '''
        table_path: str = f"{self.test_dir}/{suffix}"

        self.ydb_client.query(
            f"""
                CREATE TABLE `{table_path}` (
                    value Uint64 NOT NULL COMPRESSION({compression_settings}),
                    value1 Uint64 COMPRESSION({compression_settings}),
                    PRIMARY KEY(value),
                )
                WITH (STORE = COLUMN)
                """
        )
        logger.info(f"Table {table_path} created")
        table = ColumnTableHelper(self.ydb_client, table_path)
        self.upsert_and_wait_portions(table, self.single_upsert_rows_count, self.upsert_count)

        expected_raw = self.upsert_count * self.single_upsert_rows_count * 8
        volumes = table.get_volumes_column("value")
        assert volumes[0] == expected_raw
        assert table.get_portion_stat_by_tier()['__DEFAULT']['Rows'] == expected_raw // 8

        volumes: tuple[int, int] = table.get_volumes_column("value")
        koef: float = self.volumes_without_compression[1] / volumes[1]
        logging.info(
            f"compression in `{table.path}` {self.volumes_without_compression[1]} / {volumes[1]}: {koef}"
        )
        assert koef > 1
