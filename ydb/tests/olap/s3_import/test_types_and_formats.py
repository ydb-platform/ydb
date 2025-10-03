import logging
import pytest

from ydb.tests.library.test_meta import link_test_case
from ydb.tests.olap.s3_import.base import S3ImportTestBase

logger = logging.getLogger(__name__)


class TestTypesAndFormats(S3ImportTestBase):
    def _check_tables_hash(self, original_table_name, exported_table_name):
        result_sets = self.ydb_client.query(f"""
            SELECT
                String::Hex(Sum(Digest::MurMurHash32(Pickle(TableRow())))) AS check_hash,
                COUNT(*) AS check_size
            FROM {exported_table_name};

            SELECT
                String::Hex(Sum(Digest::MurMurHash32(Pickle(TableRow())))) AS olap_hash,
                COUNT(*) AS olap_size
            FROM {original_table_name};
        """)

        check_result = result_sets[0].rows[0]
        assert check_result.check_size > 0

        original_table_result = result_sets[1].rows[0]
        assert check_result.check_size == original_table_result.olap_size
        assert check_result.check_hash == original_table_result.olap_hash

    @link_test_case("#18784")
    @pytest.mark.parametrize(
        "format",
        [
            "csv_with_names",
            "tsv_with_names",
            "json_each_row"
        ]
    )
    @pytest.mark.parametrize(
        "compression",
        [
            "gzip",
            "zstd",
            "lz4",
            "brotli",
            "bzip2",
            "xz"
        ]
    )
    def test_different_types_and_formats(self, format, compression):
        olap_table_name = f"olap_table_{format}_{compression}"
        s3_source_name = f"s3_source_{format}_{compression}"
        s3_table_name = f"s3_table_{format}_{compression}"
        from_s3_table_name = f"from_s3_{format}_{compression}"

        table_schema = """
            c_int8 Int8,
            c_int16 Int16,
            c_int32 Int32 NOT NULL,
            c_int64 Int64,
            c_uint8 Uint8,
            c_uint16 Uint16,
            c_uint32 Uint32,
            c_uint64 Uint64,
            c_float Float,
            c_double Double,
            c_string String,
            c_utf8 Utf8,
            c_json Json,
            c_date Date,
            c_datetime Datetime,
            c_timestamp Timestamp
        """

        self.ydb_client.query(f"""
            CREATE TABLE {olap_table_name} (
                {table_schema},
                PRIMARY KEY (c_int32)
            ) WITH (
                STORE = COLUMN
            );
        """)

        self.ydb_client.query(f"""
            UPSERT INTO {olap_table_name} (
                c_int8, c_int16, c_int32, c_int64, c_uint8, c_uint16, c_uint32, c_uint64, c_float, c_double, c_string, c_utf8, c_json, c_date, c_datetime, c_timestamp
            ) VALUES
                (1, 1, 1, 1, 1, 1, 1, 1, Float("0.5"), Double("-0.5"), "hello", "world", Json("[10, 20, 30]"), Date("2025-08-25"), Datetime("2025-08-25T10:00:00Z"), Timestamp("2025-08-25T10:00:00Z")),
                (NULL, NULL, 2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
        """)

        test_bucket = f"{format}_{compression}_bucket"
        self.s3_client.create_bucket(test_bucket)

        access_key_id_secret_name = f"{test_bucket}_key_id"
        access_key_secret_secret_name = f"{test_bucket}_key_secret"
        self.ydb_client.query(f"CREATE OBJECT {access_key_id_secret_name} (TYPE SECRET) WITH value='{self.s3_client.key_id}'")
        self.ydb_client.query(f"CREATE OBJECT {access_key_secret_secret_name} (TYPE SECRET) WITH value='{self.s3_client.key_secret}'")

        self.ydb_client.query(f"""
            CREATE EXTERNAL DATA SOURCE {s3_source_name} WITH (
                SOURCE_TYPE = "ObjectStorage",
                LOCATION = "{self.s3_mock.endpoint}/{test_bucket}",
                AUTH_METHOD="AWS",
                AWS_ACCESS_KEY_ID_SECRET_NAME="{access_key_id_secret_name}",
                AWS_SECRET_ACCESS_KEY_SECRET_NAME="{access_key_secret_secret_name}",
                AWS_REGION="{self.s3_client.region}"
            );

            CREATE EXTERNAL TABLE {s3_table_name} (
                {table_schema}
            ) WITH (
                DATA_SOURCE="{s3_source_name}",
                LOCATION="/test_folder/",
                FORMAT="{format}",
                COMPRESSION="{compression}"
            );
        """)

        logger.info("Exporting into s3...")
        self.ydb_client.query(f"INSERT INTO {s3_table_name} SELECT * FROM {olap_table_name}")
        logger.info(f"Exporting finished, bucket stats: {self.s3_client.get_bucket_stat(test_bucket)}")

        logger.info("Importing into ydb...")
        self.ydb_client.query(f"""
            CREATE TABLE {from_s3_table_name} (
                PRIMARY KEY (c_int32)
            ) WITH (
                STORE = COLUMN
            ) AS SELECT * FROM {s3_table_name}
        """)

        self._check_tables_hash(olap_table_name, from_s3_table_name)

    @link_test_case("#18784")
    def test_parquet_format(self):
        olap_table_name = "olap_table"
        s3_source_name = "s3_source"
        s3_table_name = "s3_table"
        from_s3_table_name = "from_s3"

        table_schema = """
            c_int8 Int8,
            c_int16 Int16,
            c_int32 Int32 NOT NULL,
            c_int64 Int64,
            c_uint8 Uint8,
            c_uint16 Uint16,
            c_uint32 Uint32,
            c_uint64 Uint64,
            c_float Float,
            c_double Double,
            c_string String,
            c_utf8 Utf8,
            c_json Json,
            c_date Date,
            c_date32 Date32,
            c_datetime Datetime,
            c_datetime64 Datetime64,
            c_timestamp Timestamp,
            c_timestamp64 Timestamp64,
            c_decimal Decimal(22, 9)
        """

        self.ydb_client.query(f"""
            CREATE TABLE {olap_table_name} (
                {table_schema},
                PRIMARY KEY (c_int32)
            ) WITH (
                STORE = COLUMN
            );
        """)

        self.ydb_client.query(f"""
            UPSERT INTO {olap_table_name} (
                c_int8,
                c_int16,
                c_int32,
                c_int64,
                c_uint8,
                c_uint16,
                c_uint32,
                c_uint64,
                c_float,
                c_double,
                c_string,
                c_utf8,
                c_json,
                c_date,
                c_date32,
                c_datetime,
                c_datetime64,
                c_timestamp,
                c_timestamp64,
                c_decimal
            ) VALUES
                (
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    Float("0.5"),
                    Double("-0.5"),
                    "hello",
                    "world",
                    Json("[10, 20, 30]"),
                    Date("2025-08-25"),
                    Date32("2025-08-25"),
                    Datetime("2025-08-25T10:00:00Z"),
                    Datetime64("2025-08-25T10:00:00Z"),
                    Timestamp("2025-08-25T10:00:00Z"),
                    Timestamp64("2025-08-25T10:00:00Z"),
                    CAST("12.34" AS Decimal(22, 9))
                ),
                (
                    NULL,
                    NULL,
                    2,
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    NULL
                )
        """)

        test_bucket = "parquet_bucket"
        self.s3_client.create_bucket(test_bucket)

        access_key_id_secret_name = f"{test_bucket}_key_id"
        access_key_secret_secret_name = f"{test_bucket}_key_secret"
        self.ydb_client.query(f"CREATE OBJECT {access_key_id_secret_name} (TYPE SECRET) WITH value='{self.s3_client.key_id}'")
        self.ydb_client.query(f"CREATE OBJECT {access_key_secret_secret_name} (TYPE SECRET) WITH value='{self.s3_client.key_secret}'")

        self.ydb_client.query(f"""
            CREATE EXTERNAL DATA SOURCE {s3_source_name} WITH (
                SOURCE_TYPE = "ObjectStorage",
                LOCATION = "{self.s3_mock.endpoint}/{test_bucket}",
                AUTH_METHOD="AWS",
                AWS_ACCESS_KEY_ID_SECRET_NAME="{access_key_id_secret_name}",
                AWS_SECRET_ACCESS_KEY_SECRET_NAME="{access_key_secret_secret_name}",
                AWS_REGION="{self.s3_client.region}"
            );

            CREATE EXTERNAL TABLE {s3_table_name} (
                {table_schema}
            ) WITH (
                DATA_SOURCE="{s3_source_name}",
                LOCATION="/test_folder/",
                FORMAT="parquet"
            );
        """)

        logger.info("Exporting into s3...")
        self.ydb_client.query(f"""
            PRAGMA s3.UseBlocksSink = "true";

            INSERT INTO {s3_table_name} SELECT * FROM {olap_table_name};
        """)
        logger.info(f"Exporting finished, bucket stats: {self.s3_client.get_bucket_stat(test_bucket)}")

        logger.info("Importing into ydb...")
        self.ydb_client.query(f"""
            CREATE TABLE {from_s3_table_name} (
                PRIMARY KEY (c_int32)
            ) WITH (
                STORE = COLUMN
            ) AS SELECT * FROM {s3_table_name}
        """)

        self._check_tables_hash(olap_table_name, from_s3_table_name)
