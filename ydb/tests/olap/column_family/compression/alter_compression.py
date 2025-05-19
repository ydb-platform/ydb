import logging
from .base import ColumnFamilyTestBase
from typing import Callable
from ydb.tests.library.common.helpers import plain_or_under_sanitizer
from ydb.tests.olap.common.thread_helper import TestThread, TestThreads
from ydb.tests.olap.common.column_table_helper import ColumnTableHelper

logger = logging.getLogger(__name__)


class TestAlterCompression(ColumnFamilyTestBase):
    class_name = "alter_compression"

    @classmethod
    def setup_class(cls):
        super(TestAlterCompression, cls).setup_class()

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

    def add_family_in_create(self, name: str, settings: str):
        return f"FAMILY {name} ({settings})"

    def test_all_supported_compression(self):
        ''' Implements https://github.com/ydb-platform/ydb/issues/13640 '''

        single_upsert_rows_count: int = 10**5
        upsert_count: int = 10
        test_name: str = "all_supported_compression"
        test_dir: str = f"{self.ydb_client.database}/{self.class_name}/{test_name}"
        tables_path: list[str] = [
            f"{test_dir}/off_compression",
            f"{test_dir}/lz4_compression",
            f"{test_dir}/zstd_compression",
        ]
        add_default_family: Callable[[str], str] = lambda settings: self.add_family_in_create(name='default', settings=settings)
        tables_family: list[str] = [
            add_default_family('COMPRESSION = "off"'),
            add_default_family('COMPRESSION = "lz4"'),
            add_default_family('COMPRESSION = "zstd"'),
        ]

        for i in range(2, 22):
            tables_path.append(f"{test_dir}/zstd_{i}_compression")
            tables_family.append(add_default_family(f'COMPRESSION = "zstd", COMPRESSION_LEVEL = {i}'))

        assert len(tables_path) == len(tables_family)

        tables: list[ColumnTableHelper] = []
        for table_path, table_family in zip(tables_path, tables_family):
            self.ydb_client.query(
                f"""
                CREATE TABLE `{table_path}` (
                    value Uint64 NOT NULL,
                    value1 Uint64,
                    PRIMARY KEY(value),
                    {table_family}
                )
                WITH (STORE = COLUMN)
                """
            )
            logger.info(f"Table {table_path} created")
            tables.append(ColumnTableHelper(self.ydb_client, table_path))

        assert len(tables) == len(tables_path)

        tasks: TestThreads = TestThreads()
        for table in tables:
            tasks.append(TestThread(target=self.upsert_and_wait_portions, args=[table, single_upsert_rows_count, upsert_count]))

        tasks.start_and_wait_all()

        expected_raw = upsert_count * single_upsert_rows_count * 8
        volumes_without_compression: tuple[int, int] = tables[0].get_volumes_column("value")

        for table in tables:
            volumes = table.get_volumes_column("value")
            assert volumes[0] == expected_raw
            assert table.get_portion_stat_by_tier()['__DEFAULT']['Rows'] == expected_raw // 8

        for i in range(1, len(tables_path)):
            volumes: tuple[int, int] = tables[i].get_volumes_column("value")
            koef: float = volumes_without_compression[1] / volumes[1]
            logging.info(
                f"compression in `{tables[i].path}` {volumes_without_compression[1]} / {volumes[1]}: {koef}"
            )
            assert koef > 1

    def test_availability_data(self):
        ''' Implements https://github.com/ydb-platform/ydb/issues/13643 '''

        single_upsert_rows_count: int = 10 ** 2
        upsert_rows_count: int = 10
        rows_count: int = 0
        test_name: str = "availability_data"
        test_dir: str = f"{self.ydb_client.database}/{self.class_name}/{test_name}"
        tables_path: str = f"{test_dir}/test_table"

        tables_family: list[str] = [
            self.add_family_in_create(name='default', settings='COMPRESSION = "off"'),
            self.add_family_in_create(name='family_lz4', settings='COMPRESSION = "lz4"'),
            self.add_family_in_create(name='family_zstd', settings='COMPRESSION = "zstd"'),
            self.add_family_in_create(name='family_zstd_10', settings='COMPRESSION = "zstd", COMPRESSION_LEVEL = 10'),
        ]

        self.ydb_client.query(
            f"""
            CREATE TABLE `{tables_path}` (
                value Uint64 NOT NULL,
                value1 Uint64,
                PRIMARY KEY(value),
                {','.join(tables_family)}
            )
            WITH (STORE = COLUMN)
            """
        )
        logger.info(f"Table {tables_path} created")
        test_table: ColumnTableHelper = ColumnTableHelper(self.ydb_client, tables_path)

        def check_data(table: ColumnTableHelper, rows_cont: int):
            assert table.get_row_count() == rows_cont
            count_row: int = 0
            result_set = self.ydb_client.query(f"SELECT * FROM `{table.path}` ORDER BY `value`")
            for result in result_set:
                logging.info(result)
                for row in result.rows:
                    assert row['value'] == count_row and row['value1'] == count_row
                    count_row += 1
            assert count_row == rows_cont

        self.upsert_and_wait_portions(test_table, single_upsert_rows_count, upsert_rows_count)
        rows_count += single_upsert_rows_count * upsert_rows_count
        check_data(table=test_table, rows_cont=rows_count)

        self.ydb_client.query(f"ALTER TABLE `{tables_path}` ALTER COLUMN `value` SET FAMILY family_lz4")
        self.upsert_and_wait_portions(test_table, single_upsert_rows_count, upsert_rows_count)
        rows_count += single_upsert_rows_count * upsert_rows_count
        check_data(table=test_table, rows_cont=rows_count)

        self.ydb_client.query(f"ALTER TABLE `{tables_path}` ALTER COLUMN `value1` SET FAMILY family_zstd")
        self.upsert_and_wait_portions(test_table, single_upsert_rows_count, upsert_rows_count)
        rows_count += single_upsert_rows_count * upsert_rows_count
        check_data(table=test_table, rows_cont=rows_count)

        self.ydb_client.query(f"ALTER TABLE `{tables_path}` ALTER COLUMN `value` SET FAMILY family_zstd_10")
        self.ydb_client.query(f"ALTER TABLE `{tables_path}` ALTER COLUMN `value1` SET FAMILY family_zstd_10")
        self.upsert_and_wait_portions(test_table, single_upsert_rows_count, upsert_rows_count)
        rows_count += single_upsert_rows_count * upsert_rows_count
        check_data(table=test_table, rows_cont=rows_count)
