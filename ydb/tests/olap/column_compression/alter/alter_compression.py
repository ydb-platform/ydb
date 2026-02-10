import logging
import pytest
import time
from ydb.tests.olap.column_compression.common.base import ColumnTestBase
from ydb.tests.library.common.helpers import plain_or_under_sanitizer
from ydb.tests.olap.common.column_table_helper import ColumnTableHelper

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
                        value1: $i / 3,
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
        cls.single_upsert_rows_count: int = 10000
        cls.upsert_count: int = 10
        cls.volumes_without_compression: tuple[int, int]
        cls.test_name: str = "all_supported_compression"
        cls.test_dir: str = f"{cls.ydb_client.database}/{cls.class_name}/{cls.test_name}"

    COMPRESSION_CASES = [
        ("default",          ''),
        ("lz4_compression",  'algorithm=lz4'),
        ("zstd_compression", 'algorithm=zstd'),
    ] + [
        (f"zstd_{lvl}_compression", f'algorithm=zstd,level={lvl}')
        for lvl in range(2, 22, 3)
    ]

    def create_table_without_compression(self, suffix: str):
        self.table_path: str = f"{self.test_dir}/to_compress_with_{suffix}"

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

        query = f"""ALTER OBJECT `{self.table_path}` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`lc-buckets`, `COMPACTION_PLANNER.FEATURES`=`
        {{"levels" : [{{"class_name" : "Zero", "portions_live_duration" : "30", "portions_count_available" : 0, "expected_portion_size" : 10000000}},
                      {{"class_name" : "OneLayer", "expected_portion_size" : 10000000}}]}}`)"""
        self.ydb_client.query(query)

        table = ColumnTableHelper(self.ydb_client, self.table_path)
        self.upsert_and_wait_portions(table, self.single_upsert_rows_count, self.upsert_count)

        expected_raw = self.upsert_count * self.single_upsert_rows_count * 8
        self.volumes_without_compression = table.get_volumes_column("value1")

        volumes = table.get_volumes_column("value1")
        assert volumes[0] == expected_raw
        assert table.get_portion_stat_by_tier()['__DEFAULT']['Rows'] == expected_raw // 8

    @pytest.mark.parametrize("suffix, compression_settings", COMPRESSION_CASES)
    def test_alter_to_compression(self, suffix: str, compression_settings: str):
        ''' Implements https://github.com/ydb-platform/ydb/issues/13640 '''
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
        self.ydb_client.query(
            f"""
                UPDATE `{self.table_path}` SET value1 = value1 + 1;
            """
        )

        # wait for compaction
        time.sleep(60)

        result = self.ydb_client.query(
            f"""
                select * from `{self.table_path}/.sys/primary_index_portion_stats`;
            """
        )

        # check one portion left
        assert(len(result[0].rows) == 1)

        table = ColumnTableHelper(self.ydb_client, self.table_path)

        volumes = table.get_volumes_column("value1")
        koef = self.volumes_without_compression[1] / volumes[1]
        logging.info(
            f"compression in `{table.path}` {self.volumes_without_compression[1]} / {volumes[1]}: {koef}"
        )
        assert koef > 1
