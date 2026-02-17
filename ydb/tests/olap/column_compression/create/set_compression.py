import logging
import pytest
import random
from ydb.tests.olap.column_compression.common.base import ColumnTestBase
from ydb.tests.library.common.helpers import plain_or_under_sanitizer
from ydb.tests.olap.common.column_table_helper import ColumnTableHelper
from ydb.tests.library.common.protobuf_ss import SchemeDescribeRequest

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
                        key: $i + $prev_count,
                        vInt: $i / 3,
                        vStr: 'qwerty'u || CAST($i %% 8 as Utf8),
                        vFlt: $i %% 10,
                        vTs: DateTime::FromSeconds(CAST($i AS Uint32))
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


class TestCreateWithColumnCompression(TestCompressionBase):
    class_name = "create_with_column_compression"

    @classmethod
    def setup_class(cls):
        super(TestCreateWithColumnCompression, cls).setup_class()
        cls.single_upsert_rows_count = 1000
        cls.upsert_count = 10
        cls.volumes_without_compression = {}
        cls.test_name = "all_supported_compression"
        cls.test_dir = f"{cls.ydb_client.database}/{cls.class_name}/{cls.test_name}"

    COMPRESSION_CASES = [
        ("default",          '',                                   0),
        ("lz4_compression",  'algorithm=lz4',                      1),
        ("zstd_compression", 'algorithm=zstd',                     2),
    ] + [
        (f"zstd_{lvl}_compression", f'algorithm=zstd,level={lvl}', 2)
        for lvl in range(2, 22)
    ]

    def create_table_without_compression(self, suffix):
        self.table_path = f"{self.test_dir}/no_compression_{suffix}_{random.randint(1000, 9999)}"

        self.ydb_client.query(
            f"""
                CREATE TABLE `{self.table_path}` (
                    key Uint64 NOT NULL COMPRESSION(algorithm=off),
                    vInt Uint64 COMPRESSION(algorithm=off),
                    vStr Utf8 COMPRESSION(algorithm=off),
                    vFlt Float COMPRESSION(algorithm=off),
                    vTs Timestamp COMPRESSION(algorithm=off),
                    PRIMARY KEY(key),
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
        for col in ["vInt", "vStr", "vFlt", "vTs"]:
            self.volumes_without_compression[col] = table.get_volumes_column(col)[1]

        actual_rows = table.get_portion_stat_by_tier()['__DEFAULT']['Rows']
        assert actual_rows == expected_raw // 8

    @pytest.mark.parametrize("suffix, compression_settings, codec_id", COMPRESSION_CASES)
    def test_create_with_compression(self, suffix, compression_settings, codec_id):
        ''' Implements https://github.com/ydb-platform/ydb/issues/13626 '''
        self.create_table_without_compression(suffix)
        compressed_table_path = f"{self.test_dir}/compressed_{suffix}"

        self.ydb_client.query(
            f"""
                CREATE TABLE `{compressed_table_path}` (
                    key Uint64 NOT NULL COMPRESSION({compression_settings}),
                    vInt Uint64 COMPRESSION({compression_settings}),
                    vStr Utf8 COMPRESSION({compression_settings}),
                    vFlt Float COMPRESSION({compression_settings}),
                    vTs Timestamp COMPRESSION({compression_settings}),
                    PRIMARY KEY(key),
                )
                WITH (
                    STORE = COLUMN,
                    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1,
                    PARTITION_COUNT = 1
                )
                """
        )
        logger.info(f"Table {compressed_table_path} created")

        columns = self.cluster.client.send(
            SchemeDescribeRequest(compressed_table_path).protobuf,
            'SchemeDescribe').PathDescription.ColumnTableDescription.Schema.Columns

        for column in columns:
            if codec_id == 0:
                assert column.HasField("Serializer") is False
            else:
                assert column.Serializer.ArrowCompression.Codec == codec_id

        table = ColumnTableHelper(self.ydb_client, compressed_table_path)
        self.upsert_and_wait_portions(table, self.single_upsert_rows_count, self.upsert_count)

        for col in ["vInt", "vStr", "vFlt", "vTs"]:
            volumes = table.get_volumes_column(col)
            koef: float = self.volumes_without_compression[col] / volumes[1]
            logging.info(
                f"compression in `{table.path}` {self.volumes_without_compression[col]} / {volumes[1]}: {koef}"
            )
            assert koef > 1, col

    @pytest.mark.parametrize("suffix, compression_settings, codec_id", COMPRESSION_CASES)
    def test_add_with_compression(self, suffix, compression_settings, codec_id):
        ''' Implements https://github.com/ydb-platform/ydb/issues/13626 '''
        self.create_table_without_compression(suffix)

        for col, t in [("vInt", "Uint64"), ("vStr", "Utf8"), ("vFlt", "Float") , ("vTs", "Timestamp")]:
            self.ydb_client.query(
                f"""
                    ALTER TABLE `{self.table_path}`
                        ADD COLUMN `{col}2` {t} COMPRESSION({compression_settings});
                """
            )
            self.ydb_client.query(
                f"UPDATE `{self.table_path}` SET `{col}2` = `{col}`;"
            )

        table = ColumnTableHelper(self.ydb_client, self.table_path)
        current_num_rows: int = table.get_row_count()
        logger.info(f"Table {self.table_path} altered, rows: {current_num_rows}")

        for col in ["vInt", "vStr", "vFlt", "vTs"]:
            col2 = col + "2"
            volumes = table.get_columns_bytes(col, col2)
            koef: float = volumes[col]["BlobRangeSize"] / volumes[col2]["BlobRangeSize"]
            logging.info(
                f"column `{col2}` compression in `{table.path}` {volumes[col]["BlobRangeSize"]} / {volumes[col2]["BlobRangeSize"]}: {koef}"
            )
            assert koef > 1, col
