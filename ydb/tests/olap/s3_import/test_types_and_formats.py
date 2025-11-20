import io
import logging
import pytest
import ydb

import pyarrow.parquet as pq

from ydb.tests.library.test_meta import link_test_case
from ydb.tests.olap.s3_import.base import S3ImportTestBase

logger = logging.getLogger(__name__)

YQL_TO_ARROW_TYPE_MAPPING = {
    "Bool": ("UINT8", ["BOOL", "UINT8"]),
    "Int8": ("INT8", ["INT8"]),
    "Int16": ("INT16", ["INT16"]),
    "Int32": ("INT32", ["INT32"]),
    "Int64": ("INT64", ["INT64"]),
    "Uint8": ("UINT8", ["UINT8", "INT8", "INT16", "INT32", "INT64"]),
    "Uint16": ("UINT16", ["UINT16", "INT16", "INT32", "INT64"]),
    "Uint32": ("UINT32", ["UINT32", "INT32", "INT64"]),
    "Uint64": ("UINT64", ["UINT64", "INT64"]),
    "Float": ("FLOAT32", ["FLOAT32"]),
    "Double": ("FLOAT64", ["FLOAT64"]),
    "String": ("BINARY", ["BINARY"]),
    "Utf8": ("BINARY", ["BINARY"]),
    "Json": ("BINARY", ["BINARY"]),
    "Date": ("UINT16", ["UINT16", "INT32", "UINT32", "INT64", "UINT64", "DATE", "TIMESTAMP"]),
    "Date32": ("DATE", ["DATE"]),
    "Datetime": ("UINT32", ["UINT16", "INT32", "UINT32", "INT64", "UINT64", "DATE", "TIMESTAMP"]),
    "Datetime64": ("TIMESTAMP", ["TIMESTAMP"]),
    "Timestamp": ("TIMESTAMP", ["TIMESTAMP"]),
    "Timestamp64": ("TIMESTAMP", ["TIMESTAMP"]),
    "Decimal": ("DECIMAL", ["DECIMAL"]),
}


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

    def _check_arrow_types_in_parquet(self, bucket_name, yql_column_name_to_type):
        bucket = self.s3_client.s3.Bucket(bucket_name)
        parquet_files = [obj for obj in bucket.objects.all() if obj.key.endswith('.parquet')]

        assert len(parquet_files) > 0, f"No parquet files found in bucket {bucket_name}"

        for parquet_file in parquet_files:
            obj = self.s3_client.client.get_object(Bucket=bucket_name, Key=parquet_file.key)
            parquet_data = io.BytesIO(obj['Body'].read())
            parquet_file_obj = pq.ParquetFile(parquet_data)
            schema = parquet_file_obj.schema_arrow

            logger.info(f"Checking Arrow types in {parquet_file.key}")
            logger.info(f"Schema: {schema}")
            schema_fields_info = [(f.name, str(f.type), f.type.__class__.__name__ if hasattr(f.type, '__class__') else 'N/A') for f in schema]
            logger.info(f"Schema fields: {schema_fields_info}")

            for field in schema:
                column_name = field.name
                arrow_type = field.type

                if column_name not in yql_column_name_to_type:
                    continue

                yql_type = yql_column_name_to_type[column_name]

                if yql_type not in YQL_TO_ARROW_TYPE_MAPPING:
                    logger.warning(f"Unknown YQL type {yql_type} for column {column_name}, skipping")
                    continue

                expected_export_type, expected_import_types = YQL_TO_ARROW_TYPE_MAPPING[yql_type]

                arrow_type_str = str(arrow_type)
                if hasattr(arrow_type, '__class__'):
                    type_class_name = arrow_type.__class__.__name__
                    if type_class_name.endswith('Type'):
                        type_class_name = type_class_name[:-4]
                    type_class_name_lower = type_class_name.lower()
                else:
                    type_class_name_lower = arrow_type_str.lower()

                logger.debug(f"Column {column_name}: type_class_name={type_class_name if hasattr(arrow_type, '__class__') else 'N/A'}, "
                             f"type_class_name_lower={type_class_name_lower}, arrow_type_str={arrow_type_str}")

                arrow_type_mapping = {
                    'float32': 'FLOAT32',
                    'float64': 'FLOAT64',
                    'int8': 'INT8',
                    'int16': 'INT16',
                    'int32': 'INT32',
                    'int64': 'INT64',
                    'uint8': 'UINT8',
                    'uint16': 'UINT16',
                    'uint32': 'UINT32',
                    'uint64': 'UINT64',
                    'float': 'FLOAT32',
                    'double': 'FLOAT64',
                    'binary': 'BINARY',
                    'string': 'BINARY',
                    'utf8': 'BINARY',
                    'bool': 'BOOL',
                    'date32': 'DATE',
                    'date': 'DATE',
                    'timestamp': 'TIMESTAMP',
                    'decimal128': 'DECIMAL',
                    'decimal': 'DECIMAL',
                }

                normalized_arrow_type = arrow_type_mapping.get(type_class_name_lower)

                if normalized_arrow_type is None:
                    arrow_type_lower = arrow_type_str.lower()
                    priority_keys = ['float32', 'float64', 'uint8', 'uint16', 'uint32', 'uint64',
                                     'int8', 'int16', 'int32', 'int64', 'date32', 'decimal128']
                    for key in priority_keys:
                        if arrow_type_lower == key or arrow_type_lower.startswith(key + '[') or arrow_type_lower.startswith(key + '('):
                            normalized_arrow_type = arrow_type_mapping.get(key)
                            if normalized_arrow_type:
                                break

                    if normalized_arrow_type is None:
                        for key, value in arrow_type_mapping.items():
                            if key not in priority_keys:
                                if arrow_type_lower == key or arrow_type_lower.startswith(key + '[') or arrow_type_lower.startswith(key + '('):
                                    normalized_arrow_type = value
                                    break

                if normalized_arrow_type is None:
                    if hasattr(arrow_type, '__class__'):
                        type_name = arrow_type.__class__.__name__
                        if type_name.endswith('Type'):
                            type_name = type_name[:-4]
                        normalized_arrow_type = type_name.upper()
                    else:
                        normalized_arrow_type = arrow_type_str.upper()
                        if '[' in normalized_arrow_type:
                            normalized_arrow_type = normalized_arrow_type.split('[')[0]
                        if '(' in normalized_arrow_type:
                            normalized_arrow_type = normalized_arrow_type.split('(')[0]
                        normalized_arrow_type = normalized_arrow_type.strip()

                expected_export_upper = expected_export_type.upper()
                expected_import_uppers = [t.upper() for t in expected_import_types]
                is_valid = (normalized_arrow_type == expected_export_upper or
                            normalized_arrow_type in expected_import_uppers)

                assert is_valid, \
                    f"Column {column_name} (YQL type: {yql_type}): " \
                    f"expected Arrow export type {expected_export_upper} or import types {expected_import_types}, " \
                    f"got {normalized_arrow_type} (raw type: {arrow_type}, type_str: {arrow_type_str})"

                if normalized_arrow_type == expected_export_upper:
                    logger.info(f"Column {column_name}: YQL {yql_type} -> Arrow {normalized_arrow_type} (export)")
                else:
                    logger.info(f"Column {column_name}: YQL {yql_type} -> Arrow {normalized_arrow_type} "
                                f"(export expected: {expected_export_upper}, but got valid import type)")

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
            c_timestamp Timestamp,
            c_bool Bool
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
                c_int8, c_int16, c_int32, c_int64, c_uint8, c_uint16, c_uint32, c_uint64,
                c_float, c_double, c_string, c_utf8, c_json, c_date, c_datetime, c_timestamp, c_bool
            ) VALUES
                (1, 1, 1, 1, 1, 1, 1, 1, Float("0.5"), Double("-0.5"), "hello", "world",
                 Json("[10, 20, 30]"), Date("2025-08-25"), Datetime("2025-08-25T10:00:00Z"),
                 Timestamp("2025-08-25T10:00:00Z"), true),
                (NULL, NULL, 2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                 NULL, NULL, NULL, NULL, false);
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
        olap_table_name = "olap_table_parquet"
        s3_source_name = "s3_source_parquet"
        s3_table_name = "s3_table_parquet"
        from_s3_table_name = "from_s3_parquet"

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
            c_decimal Decimal(22, 9),
            c_bool Bool
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
                c_decimal,
                c_bool
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
                    CAST("12.34" AS Decimal(22, 9)),
                    true
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
                    NULL,
                    false
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

        yql_column_types = {
            "c_int8": "Int8",
            "c_int16": "Int16",
            "c_int32": "Int32",
            "c_int64": "Int64",
            "c_uint8": "Uint8",
            "c_uint16": "Uint16",
            "c_uint32": "Uint32",
            "c_uint64": "Uint64",
            "c_float": "Float",
            "c_double": "Double",
            "c_string": "String",
            "c_utf8": "Utf8",
            "c_json": "Json",
            "c_date": "Date",
            "c_date32": "Date32",
            "c_datetime": "Datetime",
            "c_datetime64": "Datetime64",
            "c_timestamp": "Timestamp",
            "c_timestamp64": "Timestamp64",
            "c_decimal": "Decimal",
            "c_bool": "Bool",
        }

        self._check_arrow_types_in_parquet(test_bucket, yql_column_types)

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
    def test_parquet_datetime_types(self):
        olap_table_name = "olap_table_datetime"
        s3_source_name = "s3_source_datetime"
        s3_table_name = "s3_table_datetime"
        from_s3_table_name = "from_s3_datetime"

        table_schema = """
            c_int32 Int32 NOT NULL,
            c_date Date,
            c_date32 Date32,
            c_datetime Datetime,
            c_datetime64 Datetime64,
            c_timestamp Timestamp,
            c_timestamp64 Timestamp64
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
            UPSERT INTO {olap_table_name} (c_int32, c_date, c_date32, c_datetime, c_datetime64, c_timestamp, c_timestamp64)
            VALUES
                (
                    1,
                    Date("2025-08-25"),
                    Date32("2025-08-25"),
                    Datetime("2025-08-25T10:00:00Z"),
                    Datetime64("2025-08-25T10:00:00Z"),
                    Timestamp("2025-08-25T10:00:00Z"),
                    Timestamp64("2025-08-25T10:00:00Z")
                ),
                (
                    2,
                    Date("1970-01-01"),
                    Date32("1900-01-01"),
                    Datetime("1970-01-01T00:00:00Z"),
                    Datetime64("1900-01-01T00:00:00Z"),
                    Timestamp("1970-01-01T00:00:00Z"),
                    Timestamp64("1900-01-01T00:00:00Z")
                ),
                (
                    3,
                    Date("2100-01-01"),
                    Date32("3100-01-01"),
                    Datetime("2100-01-01T00:00:00Z"),
                    Datetime64("3100-01-01T00:00:00Z"),
                    Timestamp("2100-01-01T00:00:00Z"),
                    Timestamp64("3100-01-01T00:00:00Z")
                ),
                (
                    4,
                    Date("1970-01-01"),
                    Date32("0001-01-01"),
                    Datetime("1970-01-01T00:00:00Z"),
                    Datetime64("0001-01-01T00:00:00Z"),
                    Timestamp("1970-01-01T00:00:00Z"),
                    Timestamp64("0001-01-01T00:00:00Z")
                ),
                (
                    5,
                    Date("2105-12-31"),
                    Date32("148107-12-31"),
                    Datetime("2105-12-31T23:59:59Z"),
                    Datetime64("148107-12-31T23:59:59Z"),
                    Timestamp("2105-12-31T23:59:59Z"),
                    Timestamp64("148107-12-31T23:59:59Z")
                )
        """)

        test_bucket = "parquet_datetime_bucket"
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

        erronious_query = f"""
            UPSERT INTO {olap_table_name} (c_int32, c_date, c_date32, c_datetime, c_datetime64, c_timestamp, c_timestamp64)
            VALUES
                (
                    6,
                    Date("1969-12-31"),
                    Date32("-144170-12-31"),
                    Datetime("1969-12-31T23:59:59Z"),
                    Datetime64("-144170-12-31T23:59:59Z"),
                    Timestamp("1969-12-31T23:59:59Z"),
                    Timestamp64("-144170-12-31T23:59:59Z")
                ),
                (
                    7,
                    Date("2106-01-01"),
                    Date32("148108-01-01"),
                    Datetime("2106-01-01T00:00:00Z"),
                    Datetime64("148108-01-01T00:00:00Z"),
                    Timestamp("2106-01-01T00:00:00Z"),
                    Timestamp64("148108-01-01T00:00:00Z")
                )
        """
        expected_errors = [
            "Invalid value \\\"1969-12-31\\\" for type Date",
            "Invalid value \\\"-144170-12-31\\\" for type Date32",
            "Invalid value \\\"1969-12-31T23:59:59Z\\\" for type Datetime",
            "Invalid value \\\"-144170-12-31T23:59:59Z\\\" for type Datetime64",
            "Invalid value \\\"1969-12-31T23:59:59Z\\\" for type Timestamp",
            "Invalid value \\\"-144170-12-31T23:59:59Z\\\" for type Timestamp64",
            "Invalid value \\\"2106-01-01\\\" for type Date",
            "Invalid value \\\"148108-01-01\\\" for type Date32",
            "Invalid value \\\"2106-01-01T00:00:00Z\\\" for type Datetime",
            "Invalid value \\\"148108-01-01T00:00:00Z\\\" for type Datetime64",
            "Invalid value \\\"2106-01-01T00:00:00Z\\\" for type Timestamp",
            "Invalid value \\\"148108-01-01T00:00:00Z\\\" for type Timestamp64"
        ]
        try:
            self.ydb_client.query(erronious_query)
        except ydb.issues.GenericError as error:
            for expected_error in expected_errors:
                assert expected_error in error.message
