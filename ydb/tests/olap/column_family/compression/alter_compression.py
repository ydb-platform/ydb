import logging
from .base import ColumnFamilyTestBase, ColumnTableHelper
from typing import Callable
from ydb.tests.library.common.helpers import plain_or_under_sanitizer
from ydb.tests.olap.scenario.helpers.thread_helper import TestThread

logger = logging.getLogger(__name__)


class TestAlterCompression(ColumnFamilyTestBase):
    class_name = "alter_compression"
    single_upsert_row_count: int = 10**5
    count_upsert: int = 10

    @classmethod
    def setup_class(cls):
        super(TestAlterCompression, cls).setup_class()

    def test_all_supported_compression(self):
        test_name: str = "all_supported_compression"
        test_dir: str = f"{self.ydb_client.database}/{self.class_name}/{test_name}"
        tables_path: list[str] = [
            f"{test_dir}/off_compression",
            f"{test_dir}/lz4_compression",
            f"{test_dir}/zstd_compression",
        ]
        add_defaut_family: Callable[[str], str] = lambda settings: f"FAMILY default ({settings})"
        tables_family: list[str] = [
            add_defaut_family('COMPRESSION = "off"'),
            add_defaut_family('COMPRESSION = "lz4"'),
            add_defaut_family('COMPRESSION = "zstd"'),
        ]

        for i in range(2, 22):
            tables_path.append(f"{test_dir}/zstd_{i}_compression")
            tables_family.append(add_defaut_family(f'COMPRESSION = "zstd", COMPRESSION_LEVEL = {i}'))

        assert len(tables_path) == len(tables_family)

        tables: list[ColumnTableHelper] = []
        for i in range(len(tables_path)):
            self.ydb_client.query(
                f"""
                CREATE TABLE `{tables_path[i]}` (
                    value Uint64 NOT NULL,
                    value1 Uint64,
                    PRIMARY KEY(value),
                    {tables_family[i]}
                )
                WITH (STORE = COLUMN)
                """
            )
            logger.info(f"Table {tables_path[i]} created")
            tables.append(ColumnTableHelper(self.ydb_client, tables_path[i]))

        def upsert_and_wait_portions(table: ColumnTableHelper, number_rows_for_insert: int, count_upsert: int):
            prev_number_rows: int = table.get_row_count()
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
                    % (number_rows_for_insert, prev_number_rows, table.path)
                )
                prev_number_rows += number_rows_for_insert
                logger.info(
                    f"{prev_number_rows} rows inserted in {table.path}. portions: {table.get_portion_stat_by_tier()}, blobs: {table.get_blob_stat_by_tier()}"
                )
            assert table.get_row_count() == prev_number_rows

            if not self.wait_for(
                lambda: len(table.get_portion_stat_by_tier()) != 0, plain_or_under_sanitizer(70, 140)
            ):
                raise Exception("not all portions have been updated")

            if not self.wait_for(
                lambda: table.get_portion_stat_by_tier()['__DEFAULT']['Rows'] == self.single_upsert_row_count * self.count_upsert, plain_or_under_sanitizer(70, 140)
            ):
                raise Exception("not all portions have been updated")

        assert len(tables) == len(tables_path)

        tasks: list[TestThread] = []
        for table in tables:
            tasks.append(TestThread(target=upsert_and_wait_portions, args=[table, self.single_upsert_row_count, self.count_upsert]))

        for task in tasks:
            task.start()

        for task in tasks:
            task.join()

        volumes_without_compression: tuple[int, int] = tables[0].get_volumes_column("value")
        for table in tables:
            assert table.get_portion_stat_by_tier()['__DEFAULT']['Rows'] == self.single_upsert_row_count * self.count_upsert
            assert self.count_upsert * self.single_upsert_row_count * 8 == volumes_without_compression[0]

        for i in range(1, len(tables_path)):
            volumes: tuple[int, int] = tables[i].get_volumes_column("value")
            koef: float = volumes_without_compression[1] / volumes[1]
            logging.info(
                f"compression in `{tables[i].path}` {volumes_without_compression[1]} / {volumes[1]}: {koef}"
            )
            assert koef > 1
