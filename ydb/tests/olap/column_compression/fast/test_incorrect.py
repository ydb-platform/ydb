import logging
import os
import pytest
import yatest.common
import ydb

from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.olap.common.ydb_client import YdbClient

logger = logging.getLogger(__name__)


class TestIncorrectCompression(object):
    ''' Implements https://github.com/ydb-platform/ydb/issues/13626 '''
    test_name = "try_incorrect_compression"

    @classmethod
    def setup_class(cls):
        ydb_path = yatest.common.build_path(os.environ.get("YDB_DRIVER_BINARY"))
        logger.info(yatest.common.execute([ydb_path, "-V"], wait=True).stdout.decode("utf-8"))
        config = KikimrConfigGenerator(
            extra_feature_flags=[
                "enable_olap_compression"
            ]
        )
        cls.cluster = KiKiMR(config)
        cls.cluster.start()
        node = cls.cluster.nodes[1]
        cls.ydb_client = YdbClient(database=f"/{config.domain_name}", endpoint=f"grpc://{node.host}:{node.port}")
        cls.ydb_client.wait_connection()

        cls.test_dir = f"{cls.ydb_client.database}/{cls.test_name}"

    COMPRESSION_CASES = [
        ("just_text",         'qwerty',                         "mismatched input"),
        ("unknown_key",       'codec=lz4',                      "Only algorithm and level settings supported"),
        ("unknown_algorithm", 'algorithm=rar',                  "Unknown compression algorithm"),
        ("only_level",        'level=3',                        "specified without an algorithm"),
        ("negative_level",    'algorithm=zstd, level=-1',       "extraneous input"),
        ("too_big_level",     'algorithm=zstd, level=99',       "annot parse serializer"),
        ("double_algorithm",  'algorithm=lz4,algorithm=zstd',   "algorithm\\\' setting can be specified only once"),
        ("double_level",      'algorithm=zstd,level=1,level=9', "level\\\' setting can be specified only once"),
    ]

    @pytest.mark.parametrize("suffix, compression_settings, error_text", COMPRESSION_CASES)
    def test_create_with_wrong_compression(self, suffix, compression_settings, error_text):
        compressed_table_path = f"{self.test_dir}/create_{suffix}"

        try:
            self.ydb_client.query(
                f"""
                    CREATE TABLE `{compressed_table_path}` (
                        key Uint64 NOT NULL COMPRESSION({compression_settings}),
                        vStr Utf8 COMPRESSION({compression_settings}),
                        PRIMARY KEY(key),
                    )
                    WITH (
                        STORE = COLUMN
                    )
                """
            )
            assert False, 'Should Fail'
        except ydb.issues.Error as ex:
            assert error_text in ex.message

    @pytest.mark.parametrize("suffix, compression_settings, error_text", COMPRESSION_CASES)
    def test_alter_to_wrong_compression(self, suffix, compression_settings, error_text):
        table_path = f"{self.test_dir}/alter_{suffix}"

        self.ydb_client.query(
            f"""
                CREATE TABLE `{table_path}` (
                    key Uint64 NOT NULL,
                    vStr Utf8,
                    PRIMARY KEY(key),
                )
                WITH (
                    STORE = COLUMN
                )
            """
        )

        try:
            self.ydb_client.query(
                f"""
                    ALTER TABLE `{table_path}`
                        ALTER COLUMN `vStr` SET COMPRESSION({compression_settings});
                """
            )
            assert False, 'Should Fail'
        except ydb.issues.Error as ex:
            assert error_text in ex.message

    def test_create_row_based_table_with_compression(self):
        compressed_table_path = f"{self.test_dir}/create_row_table"

        try:
            self.ydb_client.query(
                f"""
                    CREATE TABLE `{compressed_table_path}` (
                        key Uint64 NOT NULL,
                        vStr Utf8 COMPRESSION(algorithm=lz4),
                        PRIMARY KEY(key),
                    )
                """
            )
            assert False, 'Should Fail'
        except ydb.issues.Error as ex:
            assert "Column Compression is not supported in row tables" in ex.message

    def test_tablestore(self):
        table_path = f"{self.test_dir}/create_tablestore"
        try:
            self.ydb_client.query(
                f"""
                CREATE TABLESTORE `{table_path}` (
                    id Uint64 NOT NULL,
                    val Int64 FAMILY fam1,
                    PRIMARY KEY(id),
                    Family default (
                        COMPRESSION = "lz4"
                    ),
                    Family fam1 (
                        COMPRESSION = "zstd"
                    ),
                )
                WITH (STORE = COLUMN);
                """
            )
            assert False, "Should Fail"
        except ydb.issues.Error as ex:
            assert "TableStore does not support column families" in ex.message
