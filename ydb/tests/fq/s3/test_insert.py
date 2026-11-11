#!/usr/bin/env python
# -*- coding: utf-8 -*-

import boto3
import logging

import pytest

import ydb.public.api.protos.ydb_value_pb2 as ydb

import ydb.tests.fq.s3.s3_helpers as s3_helpers

from moto import __version__ as moto_version
from packaging.version import Version


class TestS3(object):
    def create_bucket_and_upload_file(self, filename, s3, kikimr):
        s3_helpers.create_bucket_and_upload_file(filename, s3.s3_url, "fbucket", "ydb/tests/fq/s3/test_format_data")

    @pytest.mark.parametrize("dataset_name", ["dataset", "dataにちは% set"])
    @pytest.mark.parametrize("format", ["json_list", "json_each_row", "csv_with_names", "parquet"])
    @pytest.mark.parametrize("block_sink", ["false", "true"])
    def test_insert(self, kikimr, s3, client, format, dataset_name, unique_prefix, block_sink):
        if block_sink == "true" and format != "parquet":
            pytest.skip(f"block sink is not supported for format {format}")
            return

        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("insert_bucket")
        bucket.create(ACL='public-read-write')
        bucket.objects.all().delete()

        storage_connection_name = client.path(unique_prefix + "ibucket")
        client.create_external_data_source(storage_connection_name, "insert_bucket")

        sql = f'''
            pragma s3.UseBlocksSink = "{block_sink}";
            insert into `{storage_connection_name}`.`{dataset_name}/` with (format={format})
            select * from AS_TABLE([<|foo:123, bar:"xxx"u|>,<|foo:456, bar:"yyy"u|>]);
            '''

        query = client.execute(sql)
        query.check()

        sql = f'''
            select foo, bar from `{storage_connection_name}`.`{dataset_name}/*` with (format={format}, schema(
                foo Int NOT NULL,
                bar String NOT NULL
            ))
            '''

        query = client.execute(sql)
        query.check()

        data = query.result_set()
        result_set = data
        logging.debug(str(result_set))
        assert len(result_set.columns) == 2
        assert result_set.columns[0].name == "foo"
        assert result_set.columns[0].type.type_id == ydb.Type.INT32
        assert result_set.columns[1].name == "bar"
        assert result_set.columns[1].type.type_id == ydb.Type.STRING
        assert len(result_set.rows) == 2
        assert result_set.rows[0][0] == 123
        assert result_set.rows[0][1] == b'xxx'
        assert result_set.rows[1][0] == 456
        assert result_set.rows[1][1] == b'yyy'

    @pytest.mark.parametrize("format", ["json_each_row", "csv_with_names", "tsv_with_names"])
    @pytest.mark.parametrize("compression", ["gzip", "zstd", "lz4", "brotli", "bzip2", "xz"])
    def test_multipart_insert_with_compression(self, kikimr, s3, client, format, compression, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("insert_bucket")
        bucket.create(ACL='public-read-write')
        bucket.objects.all().delete()

        storage_connection_name = client.path(unique_prefix + "ibucket_compression")
        client.create_external_data_source(storage_connection_name, "insert_bucket")

        test_text = "hello world"
        sql = f'''
            PRAGMA s3.AtomicUploadCommit = "true";

            INSERT INTO `{storage_connection_name}`.`test/` WITH (
                FORMAT = "{format}",
                COMPRESSION = "{compression}"
            ) SELECT * FROM AS_TABLE(ListReplicate(
                <|data: "{test_text}"|>,
                1000000
            ))
        '''

        query = client.execute(sql)
        query.check()

        sql = f'''
            SELECT
                MIN(data) AS min_data,
                MAX(data) AS max_data
            FROM `{storage_connection_name}`.`test/` WITH (
                FORMAT = "{format}",
                COMPRESSION = "{compression}",
                SCHEMA (
                    data Utf8 NOT NULL
                )
            )
        '''

        query = client.execute(sql)
        query.check()

        data = query.result_set()
        result_set = data
        logging.debug(str(result_set))
        assert len(result_set.columns) == 2
        assert len(result_set.rows) == 1
        assert result_set.rows[0][0] == test_text
        assert result_set.rows[0][1] == test_text

    def test_big_json_list_insert(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("big_data_bucket")
        bucket.create(ACL='public-read-write')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        taxi = '''VendorID'''
        for i in range(37):
            taxi += "\n" + str(i)
        s3_client.put_object(Body=taxi, Bucket='big_data_bucket', Key='src/taxi.csv', ContentType='text/plain')

        connection_response = client.create_external_data_source(unique_prefix + "big_data_bucket", "big_data_bucket")

        vendorID = ydb.Column(
            name="VendorID",
            type=ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING))),
        )
        storage_source_binding_name = unique_prefix + "taxi_src_csv_with_names"
        client.create_external_table(
            name=storage_source_binding_name,
            path="src/",
            format="csv_with_names",
            data_source=connection_response.name,
            columns=[vendorID],
        )

        storage_sink_binding_name = unique_prefix + "taxi_dst_json_list_zstd"
        client.create_external_table(
            name=storage_sink_binding_name,
            path="dst/",
            format="json_list",
            compression="zstd",
            data_source=connection_response.name,
            columns=[vendorID],
        )

        client.create_external_data_source("ibucket", "insert_bucket")

        sql = f'''
            pragma s3.JsonListSizeLimit="10";
            INSERT INTO `{storage_sink_binding_name}`
            SELECT
                VendorID
            FROM `{storage_source_binding_name}`
            '''

        query = client.execute(sql)
        query.check()

        sql = f'''

            SELECT
                count(*)
            FROM `{storage_sink_binding_name}`
        '''

        query = client.execute(sql)
        query.check()

        data = query.result_set()
        result_set = data
        logging.debug(str(result_set))
        assert len(result_set.columns) == 1
        assert result_set.columns[0].name == "column0"
        assert result_set.columns[0].type.type_id == ydb.Type.UINT64
        assert len(result_set.rows) == 1
        assert result_set.rows[0][0] == 37

    def test_insert_csv_delimiter(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("insert_bucket")
        bucket.create(ACL='public-read-write')
        bucket.objects.all().delete()

        storage_connection_name = client.path(unique_prefix + "ibucket")
        client.create_external_data_source(storage_connection_name, "insert_bucket")

        sql = f'''
            insert into `{storage_connection_name}`.`csv_delim_out/` with (
              format=csv_with_names,
              csv_delimiter=";"
            )
            select * from AS_TABLE([<|foo:123, bar:"xxx"u|>,<|foo:456, bar:"yyy"u|>]);
            '''

        query = client.execute(sql)
        query.check()

        sql = f'''
            select data from `{storage_connection_name}`.`csv_delim_out/*` with (format=raw, schema(
                data String NOT NULL
            ))
            '''

        query = client.execute(sql)
        query.check()

        data = query.result_set()
        result_set = data
        logging.debug(str(result_set))
        assert len(result_set.columns) == 1
        assert result_set.columns[0].name == "data"
        assert result_set.columns[0].type.type_id == ydb.Type.STRING
        assert len(result_set.rows) == 1
        assert result_set.rows[0][0] == b'"bar";"foo"\n"xxx";123\n"yyy";456\n'

    def test_append(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("append_bucket")
        bucket.create(ACL='public-read-write')
        bucket.objects.all().delete()

        storage_connection_name = client.path(unique_prefix + "abucket")
        client.create_external_data_source(storage_connection_name, "append_bucket")

        sql = f'''
            insert into `{storage_connection_name}`.`append/` with (format=json_each_row)
            select * from AS_TABLE([<|foo:123, bar:"xxx"u|>,<|foo:456, bar:"yyy"u|>]);
            '''

        query = client.execute(sql)
        query.check()

        sql = f'''
            insert into `{storage_connection_name}`.`append/` with (format=json_each_row)
            select * from AS_TABLE([<|foo:345, bar:"zzz"u|>]);
            '''

        query = client.execute(sql)
        query.check()

        sql = f'''
            select foo, bar from `{storage_connection_name}`.`append/` with (format=json_each_row, schema(
                foo Int NOT NULL,
                bar String NOT NULL
            )) order by foo
            '''

        query = client.execute(sql)
        query.check()

        data = query.result_set()
        result_set = data
        logging.debug(str(result_set))
        assert len(result_set.columns) == 2
        assert result_set.columns[0].name == "foo"
        assert result_set.columns[0].type.type_id == ydb.Type.INT32
        assert result_set.columns[1].name == "bar"
        assert result_set.columns[1].type.type_id == ydb.Type.STRING
        assert len(result_set.rows) == 3
        assert result_set.rows[0][0] == 123
        assert result_set.rows[0][1] == b'xxx'
        assert result_set.rows[1][0] == 345
        assert result_set.rows[1][1] == b'zzz'
        assert result_set.rows[2][0] == 456
        assert result_set.rows[2][1] == b'yyy'

    def test_part_split(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("split_bucket")
        bucket.create(ACL='public-read-write')
        bucket.objects.all().delete()

        storage_connection_name = client.path(unique_prefix + "sbucket")
        client.create_external_data_source(storage_connection_name, "split_bucket")

        sql = f'''
            insert into `{storage_connection_name}`.`part/` with (format=json_each_row, partitioned_by=(foo, bar))
            select * from AS_TABLE([<|foo:123, bar:"xxx"u, data:3.14|>,<|foo:456, bar:"yyy"u, data:2.72|>,<|foo:123, bar:"xxx"u, data:1.41|>]);
            '''

        query = client.execute(sql)
        query.check()

        sql = f'''
            select data from `{storage_connection_name}`.`part/foo=123/bar=xxx/` with (format=json_each_row, schema(
                data Float NOT NULL,
            ))
            '''

        query = client.execute(sql)
        query.check()

        data = query.result_set()
        result_set = data
        logging.debug(str(result_set))
        assert len(result_set.columns) == 1
        assert result_set.columns[0].name == "data"
        assert result_set.columns[0].type.type_id == ydb.Type.FLOAT
        assert len(result_set.rows) == 2
        assert abs(result_set.rows[0][0] - 3.14) < 0.01
        assert abs(result_set.rows[1][0] - 1.41) < 0.01

    def test_part_merge(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("merge_bucket")
        bucket.create(ACL='public-read-write')
        bucket.objects.all().delete()

        storage_connection_name = client.path(unique_prefix + "mbucket")
        client.create_external_data_source(storage_connection_name, "merge_bucket")

        sql = f'''
            insert into `{storage_connection_name}`.`part/foo=123/bar=xxx/` with (format=json_each_row)
            select * from AS_TABLE([<|data:3.14|>,<|data:1.41|>]);
            '''

        query = client.execute(sql)
        query.check()

        sql = f'''
            insert into `{storage_connection_name}`.`part/foo=456/bar=yyy/` with (format=json_each_row)
            select * from AS_TABLE([<|data:2.72|>]);
            '''

        query = client.execute(sql)
        query.check()

        sql = f'''
            select foo, bar, data from `{storage_connection_name}`.`part` with (format=json_each_row, partitioned_by=(foo, bar), schema(
                foo Int NOT NULL,
                bar String NOT NULL,
                data Float NOT NULL
            )) order by foo, data
            '''

        query = client.execute(sql)
        query.check()

        data = query.result_set()
        result_set = data
        logging.debug(str(result_set))
        assert len(result_set.columns) == 3
        assert result_set.columns[0].name == "foo"
        assert result_set.columns[0].type.type_id == ydb.Type.INT32
        assert result_set.columns[1].name == "bar"
        assert result_set.columns[1].type.type_id == ydb.Type.STRING
        assert result_set.columns[2].name == "data"
        assert result_set.columns[2].type.type_id == ydb.Type.FLOAT
        assert len(result_set.rows) == 3
        assert result_set.rows[0][0] == 123
        assert result_set.rows[0][1] == b'xxx'
        assert abs(result_set.rows[0][2] - 1.41) < 0.01
        assert result_set.rows[1][0] == 123
        assert result_set.rows[1][1] == b'xxx'
        assert abs(result_set.rows[1][2] - 3.14) < 0.01
        assert result_set.rows[2][0] == 456
        assert result_set.rows[2][1] == b'yyy'
        assert abs(result_set.rows[2][2] - 2.72) < 0.01

    @pytest.mark.parametrize("format", ["json_list", "json_each_row", "csv_with_names"])
    def test_external_table_partitioning(self, kikimr, s3, client, format, unique_prefix):
        if format == "json_list":
            pytest.skip("json_list does not work with partitioned_by. YQ-1335")
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("binding_bucket")
        bucket.create(ACL='public-read-write')
        bucket.objects.all().delete()

        connection_response = client.create_external_data_source(unique_prefix + "bbucket", "binding_bucket")

        fooType = ydb.Column(name="foo", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT32))
        barType = ydb.Column(name="bar", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.UTF8))
        dataType = ydb.Column(name="data", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.DOUBLE))

        storage_binding_name = unique_prefix + "bbinding"
        client.create_external_table(
            name=storage_binding_name,
            path=format + "/",
            format=format,
            data_source=connection_response.name,
            columns=[fooType, barType, dataType],
            partitioned_by=["foo", "bar"],
            format_setting={"file_pattern": "*{json,csv}"},
        )

        sql = f'''
            insert into `{storage_binding_name}`
            select * from AS_TABLE([<|foo:123, bar:"xxx"u, data:3.14|>,<|foo:456, bar:"yyy"u, data:2.72|>,<|foo:123, bar:"xxx"u, data:1.41|>]);
            '''

        query = client.execute(sql)
        query.check()

        sql = f'''
            select foo, bar, data from `{storage_binding_name}` order by foo, data
            '''

        query = client.execute(sql)
        query.check()

        data = query.result_set()
        result_set = data
        logging.debug(str(result_set))
        assert len(result_set.columns) == 3
        assert result_set.columns[0].name == "foo"
        assert result_set.columns[0].type.type_id == ydb.Type.INT32
        assert result_set.columns[1].name == "bar"
        assert result_set.columns[1].type.type_id == ydb.Type.UTF8
        assert result_set.columns[2].name == "data"
        assert result_set.columns[2].type.type_id == ydb.Type.DOUBLE
        assert len(result_set.rows) == 3
        assert result_set.rows[0][0] == 123
        assert result_set.rows[0][1] == 'xxx'
        assert abs(result_set.rows[0][2] - 1.41) < 0.01
        assert result_set.rows[1][0] == 123
        assert result_set.rows[1][1] == 'xxx'
        assert abs(result_set.rows[1][2] - 3.14) < 0.01
        assert result_set.rows[2][0] == 456
        assert result_set.rows[2][1] == 'yyy'
        assert abs(result_set.rows[2][2] - 2.72) < 0.01

    @pytest.mark.parametrize("format", ["json_each_row", "csv_with_names", "tsv_with_names", "parquet"])
    def test_error(self, kikimr, s3, client, format, unique_prefix):
        if format == "parquet":
            pytest.skip("Transient errors do not work for arrow reader - YQ-1335")
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("error_bucket")
        acl = '' if Version(moto_version) < Version("4.0.5") else 'public-write'
        bucket.create(ACL=acl)
        bucket.objects.all().delete()

        storage_connection_name = client.path(unique_prefix + "ebucket")
        client.create_external_data_source(storage_connection_name, "error_bucket")

        sql = R'''
            insert into `{1}`.`{0}/` with (format={0})
            select * from AS_TABLE([<|foo:123, bar:"xxx"u|>,<|foo:456, bar:"yyy"u|>]);
            '''.format(format, storage_connection_name)

        query = client.execute(sql)
        query.check()

        sql = R'''
            select foo, bar from `{1}`.`{0}/` with (format={0}, schema(
                foo Int NOT NULL,
                bar String NOT NULL
            ))
            '''.format(format, storage_connection_name)

        query = client.execute(sql)
        query.check(False)
        assert "403" in query.error or "500" in query.error
        assert query.error_code in ("EXTERNAL_ERROR", "BAD_REQUEST")

    @pytest.mark.parametrize("atomic_upload", ["false", "true"])
    def test_insert_empty_object(self, kikimr, s3, client, atomic_upload, unique_prefix):
        self.create_bucket_and_upload_file("empty_file", s3, kikimr)
        data_source = client.create_external_data_source(unique_prefix + "empty_file_connection", "fbucket").name
        col = ydb.Column(name="data", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING))
        binding_name = unique_prefix + "empty_file_binding"
        client.create_external_table(
            name=binding_name, path="empty_file_path/", format="raw", data_source=data_source, columns=[col]
        )

        sql = f'''
            PRAGMA s3.AtomicUploadCommit = "{atomic_upload}";
            INSERT INTO `{binding_name}`
            SELECT "" AS data;
            '''

        query = client.execute(sql)
        query.check()

        read_query = client.execute(f"SELECT * FROM `{binding_name}`")
        read_query.check()

        data = read_query.result_set()
        result_set = data
        logging.debug(str(result_set))
        assert len(result_set.columns) == 1
        assert result_set.columns[0].name == "data"
        assert result_set.columns[0].type.type_id == ydb.Type.STRING
        assert len(result_set.rows) == 1
        assert result_set.rows[0][0] == b""

    def test_insert_without_format_error(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("insert_bucket")
        bucket.create(ACL='public-read-write')
        bucket.objects.all().delete()

        storage_connection_name = client.path(unique_prefix + "ibucket")
        client.create_external_data_source(storage_connection_name, "insert_bucket")

        sql = f'''
            insert into `{storage_connection_name}`.`/test/`
            select * from AS_TABLE([<|foo:123, bar:"xxx"u|>,<|foo:456, bar:"yyy"u|>]);
            '''

        query = client.execute(sql)
        query.check(False)
        issues = str(query.error)

        assert "No write format specified. Please use WITH FORMAT for writing into S3" in issues, (
            "Incorrect Issues: " + issues
        )

    def test_raw_format_validation(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("insert_bucket")
        bucket.create(ACL='public-read-write')
        bucket.objects.all().delete()

        storage_connection_name = client.path(unique_prefix + "ibucket")
        client.create_external_data_source(storage_connection_name, "insert_bucket")

        sql = f'''
            INSERT INTO `{storage_connection_name}`.`/test/`
            WITH (FORMAT = 'raw')
            SELECT * FROM AS_TABLE([<|foo:123, bar:"xxx"u|>,<|foo:456, bar:"yyy"u|>]);
        '''
        query = client.execute(sql)
        query.check(False)
        issues = str(query.error)
        assert "Only one column in schema supported in raw format" in issues, "Incorrect Issues: " + issues

        sql = f'''
            INSERT INTO `{storage_connection_name}`.`/test/`
            WITH (FORMAT = 'raw')
            SELECT * FROM AS_TABLE([<|foo:<|bar:"xxx"u|>|>,<|foo:<|bar:"yyy"u|>|>]);
        '''
        query = client.execute(sql)
        query.check(False)
        issues = str(query.error)
        assert "Only a column with a primitive type is allowed for the raw format" in issues, (
            "Incorrect Issues: " + issues
        )

    def get_insert_test_query(self, insert_path: str):
        sql = f"INSERT INTO {insert_path} WITH (FORMAT = \"parquet\") SELECT\n"
        for val, type in [
            (2, "Int8"),
            (-3, "Int16"),
            (4, "Int32"),
            (-5, "Int64"),
            (6, "Uint8"),
            (7, "Uint16"),
            (8, "Uint32"),
            (9, "Uint64"),
            (1, "Bool"),
            (10.5, "Float"),
            (11.9, "Double"),
            ("\"STR_1\"", "String"),
            ("\"STR_2\"", "Utf8"),
            ("\"\\\"STR_3\\\"\"", "Json"),
            ("CurrentUtcDate()", "Date"),
            ("\"2025-05-09T18:44:58Z\"", "Datetime"),
            ("\"2025-05-09T18:44:58.678906Z\"", "Timestamp"),
            ("CurrentTzDate(1)", "TzDate"),
            ("\"2025-05-09T21:45:58,Europe/Moscow\"", "TzDateTime"),
            ("\"2025-05-09T21:45:58.222886,Europe/Moscow\"", "TzTimestamp"),
        ]:
            sql += f"Unwrap(CAST({val} AS {type})) AS `{type}Col`,\n"
            sql += f"Just(Unwrap(CAST({val} AS {type}))) AS `Opt{type}Col`,\n"
            sql += f"CAST(NULL AS {type}) AS `Null{type}Col`,\n"

        logging.debug(sql)
        return sql

    def test_block_insert_enable(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("insert_bucket")
        bucket.create(ACL='public-read-write')
        bucket.objects.all().delete()

        storage_connection_name = client.path(unique_prefix + "ibucket")
        client.create_external_data_source(storage_connection_name, "insert_bucket")

        sql = self.get_insert_test_query(f"`{storage_connection_name}`.`/test/`")
        query = client.execute(sql)
        query.check()
        ast = str(query.ast)
        assert "S3SinkOutput" not in ast, "Incorrect ast: " + ast
        assert "WideToBlocks" in ast, "Incorrect ast: " + ast

        sql = f'''
            SELECT
                COUNT(*)
            FROM `{storage_connection_name}`.`/test/` WITH (
                FORMAT = "parquet",
                SCHEMA (
                    Int8Col Int8,
                )
            )
        '''

        query = client.execute(sql)
        query.check()
        result_set = query.result_set()
        assert len(result_set.columns) == 1
        assert len(result_set.rows) == 1
        assert result_set.rows[0][0] == 1

    def test_block_insert_value(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("insert_bucket")
        bucket.create(ACL='public-read-write')
        bucket.objects.all().delete()

        storage_connection_name = client.path(unique_prefix + "ibucket")
        client.create_external_data_source(storage_connection_name, "insert_bucket")

        sql = "PRAGMA s3.UseBlocksSink = \"true\";\n" + self.get_insert_test_query(
            f"`{storage_connection_name}`.`/block/`"
        )
        client.execute(sql).check()

        sql = "PRAGMA s3.UseBlocksSink = \"false\";\n" + self.get_insert_test_query(
            f"`{storage_connection_name}`.`/scalar/`"
        )
        client.execute(sql).check()

        sql = f'''
            SELECT
                *
            FROM `{storage_connection_name}`.`/` WITH (
                FORMAT = "raw",
                SCHEMA (
                    Data String,
                )
            )
        '''
        query = client.execute(sql)
        query.check()

        result_set = query.result_set()
        assert len(result_set.columns) == 1
        assert len(result_set.rows) == 2
        assert result_set.rows[0][0] == result_set.rows[1][0]

    @pytest.mark.parametrize("block_sink", ["false", "true"])
    def test_insert_deadlock(self, kikimr, s3, client, unique_prefix, block_sink):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("insert_bucket")
        bucket.create(ACL='public-read-write')
        bucket.objects.all().delete()

        storage_connection_name = client.path(unique_prefix + "ibucket")
        client.create_external_data_source(storage_connection_name, "insert_bucket")

        sql = f'''
            pragma s3.UseBlocksSink = "{block_sink}";
            pragma s3.AtomicUploadCommit = "true";
            pragma s3.InFlightMemoryLimit = "1";

            insert into `{storage_connection_name}`.`/test/`
            with (format = "parquet")
            select Random(data) from AS_TABLE(ListReplicate(<|data:"x"u|>, 1000000));
            '''

        query = client.execute(sql)
        query.check(False)
        issues = str(query.error)

        assert "Writing deadlock occurred, please increase write actor memory limit" in issues, (
            "Incorrect Issues: " + issues
        )

    def test_insert_partition_limit(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("insert_bucket")
        bucket.create(ACL='public-read-write')
        bucket.objects.all().delete()

        storage_connection_name = client.path(unique_prefix + "ibucket_partition_limit")
        client.create_external_data_source(storage_connection_name, "insert_bucket")

        sql = f'''
            PRAGMA s3.UniqueKeysCountLimit = "1";
            PRAGMA s3.SerializeMemoryLimit = "1";
            PRAGMA s3.BlockSizeMemoryLimit = "1";

            INSERT INTO `{storage_connection_name}`.`folder/` WITH (
                FORMAT = "csv_with_names",
                PARTITIONED_BY = ("key")
            ) (key, val)
            VALUES
                (1, 2),
                (2, 3)
            '''

        query = client.execute(sql)
        query.check(False)
        issues = str(query.error)

        assert "Too many unique keys: 2" in issues, "Incorrect Issues: " + issues
