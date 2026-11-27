#!/usr/bin/env python
# -*- coding: utf-8 -*-

import boto3
import logging

import pytest

import ydb.public.api.protos.ydb_value_pb2 as ydb

import ydb.tests.fq.s3.s3_helpers as s3_helpers


class TestS3Compressions:
    def create_bucket_and_upload_file(self, filename, s3, kikimr):
        s3_helpers.create_bucket_and_upload_file(
            filename, s3.s3_url, "fbucket", "ydb/tests/fq/s3/test_compression_data"
        )

    def validate_result(self, result_set):
        logging.debug(str(result_set))
        assert len(result_set.columns) == 3
        assert result_set.columns[0].name == "description"
        assert result_set.columns[0].type.type_id == ydb.Type.STRING
        assert result_set.columns[1].name == "id"
        assert result_set.columns[1].type.type_id == ydb.Type.INT32
        assert result_set.columns[2].name == "info"
        assert result_set.columns[2].type.type_id == ydb.Type.STRING
        assert len(result_set.rows) == 1
        assert result_set.rows[0][0] == b"yq"
        assert result_set.rows[0][1] == 0
        assert result_set.rows[0][2] == b"abc"

    def validate_result_csv(self, result_set):
        logging.debug(str(result_set))
        assert len(result_set.columns) == 3
        assert result_set.columns[0].name == "a"
        assert result_set.columns[0].type.optional_type.item.type_id == ydb.Type.INT64
        assert result_set.columns[1].name == "b"
        assert result_set.columns[1].type.type_id == ydb.Type.UTF8
        assert result_set.columns[2].name == "c"
        assert result_set.columns[2].type.optional_type.item.type_id == ydb.Type.DOUBLE
        assert len(result_set.rows) == 3
        assert result_set.rows[0][0] == 1
        assert result_set.rows[0][1] == "hello"
        assert result_set.rows[0][2] == 0.5
        assert result_set.rows[1][0] == 2
        assert result_set.rows[1][1] == "world"
        assert result_set.rows[1][2] == 0.25
        assert result_set.rows[2][0] == 3
        assert result_set.rows[2][1] == "!"
        assert result_set.rows[2][2] == 0.125

    @pytest.mark.parametrize(
        "filename, compression",
        [
            ("test.json.gz", "gzip"),
            ("test.json.lz4", "lz4"),
            ("test.json.br", "brotli"),
            ("test.json.bz2", "bzip2"),
            ("test.json.zst", "zstd"),
            ("test.json.xz", "xz"),
        ],
    )
    def test_compression(self, kikimr, s3, client, filename, compression, unique_prefix):
        self.create_bucket_and_upload_file(filename, s3, kikimr)
        storage_connection_name = client.path(unique_prefix + "fruitbucket")
        client.create_external_data_source(storage_connection_name, "fbucket")

        sql = '''
            SELECT *
            FROM `{}`.`{}`
            WITH (format=json_each_row, compression="{}", SCHEMA (
                id Int32 NOT NULL,
                description String NOT NULL,
                info String NOT NULL
            ));
            '''.format(storage_connection_name, filename, compression)

        query = client.execute(sql)
        query.check()

        data = query.result_set()
        result_set = data
        self.validate_result(result_set)

    @pytest.mark.parametrize(
        "filename, compression",
        [
            ("test.csv.gz", "gzip"),
            ("test.csv.lz4", "lz4"),
            ("test.csv.br", "brotli"),
            ("test.csv.bz2", "bzip2"),
            ("test.csv.zst", "zstd"),
            ("test.csv.xz", "xz"),
        ],
    )
    def test_compression_inference(self, kikimr, s3, client, filename, compression, unique_prefix):
        self.create_bucket_and_upload_file(filename, s3, kikimr)
        storage_connection_name = client.path(unique_prefix + "fruitbucket")
        client.create_external_data_source(storage_connection_name, "fbucket")

        sql = '''
            SELECT *
            FROM `{}`.`{}`
            WITH (format=csv_with_names, compression="{}", with_infer="true");
            '''.format(storage_connection_name, filename, compression)

        query = client.execute(sql)
        query.check()

        data = query.result_set()
        result_set = data
        self.validate_result_csv(result_set)

    @pytest.mark.parametrize(
        "filename, compression",
        [
            ("big.json.gz", "gzip"),
            ("big.json.lz4", "lz4"),
            ("big.json.br", "brotli"),
            ("big.json.bz2", "bzip2"),
            ("big.json.zst", "zstd"),
            ("big.json.xz", "xz"),
        ],
    )
    def test_big_compression(self, kikimr, s3, client, filename, compression, unique_prefix):
        self.create_bucket_and_upload_file(filename, s3, kikimr)
        storage_connection_name = client.path(unique_prefix + "fruitbucket")
        client.create_external_data_source(storage_connection_name, "fbucket")

        sql = '''
            SELECT count(*)
            FROM `{}`.`{}`
            WITH (format=json_each_row, compression="{}", SCHEMA (
                a String NOT NULL
            ));
            '''.format(storage_connection_name, filename, compression)

        query = client.execute(sql)
        query.check()

        data = query.result_set()
        result_set = data

        logging.debug(str(result_set))
        assert len(result_set.columns) == 1
        assert result_set.columns[0].name == "column0"
        assert result_set.columns[0].type.type_id == ydb.Type.UINT64
        assert len(result_set.rows) == 1
        assert result_set.rows[0][0] == 5458

    def test_invalid_compression(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("fbucket")
        bucket.create(ACL='public-read')

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        fruits = R'''Fruit,Price,Weight
Banana,3,100
Apple,2,22
Pear,15,33'''
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='fruits.csv', ContentType='text/plain')

        storage_connection_name = client.path(unique_prefix + "fruitbucket")
        client.create_external_data_source(storage_connection_name, "fbucket")

        sql = fR'''
            SELECT *
            FROM `{storage_connection_name}`.`fruits.csv`
            WITH (format=csv_with_names, compression="some_compression", SCHEMA (
                Fruit String,
                Price Int,
                Weight Int
            ));
            '''

        query = client.execute(sql)
        query.check(False)
        describe_result = query
        logging.debug("Describe result: {}".format(describe_result))
        describe_string = "{}".format(describe_result)
        assert (
            "Unknown compression: some_compression. Use one of: gzip, zstd, lz4, brotli, bzip2, xz" in describe_string
        )

    @pytest.mark.parametrize("format", ["json_list", "raw"])
    def test_lz4_compression_not_supported_for_read(self, kikimr, s3, client, format, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("fbucket")
        bucket.create(ACL='public-read')

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        s3_client.put_object(Body='{}', Bucket='fbucket', Key='data.json', ContentType='text/plain')

        storage_connection_name = client.path(unique_prefix + "fruitbucket")
        client.create_external_data_source(storage_connection_name, "fbucket")

        sql = fR'''
            SELECT *
            FROM `{storage_connection_name}`.`data.json`
            WITH (format={format}, compression="lz4", SCHEMA (
                key Uint64
            ));
            '''

        query = client.execute(sql)
        query.check(False)
        describe_result = query
        logging.debug("Describe result: {}".format(describe_result))
        describe_string = "{}".format(describe_result)
        assert "Compression 'lz4' is not supported for format '" + format + "'" in describe_string

    @pytest.mark.parametrize("format", ["json_list", "raw"])
    def test_lz4_compression_not_supported_for_write(self, kikimr, s3, client, format, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("insert_bucket")
        bucket.create(ACL='public-read-write')
        bucket.objects.all().delete()

        storage_connection_name = client.path(unique_prefix + "ibucket")
        client.create_external_data_source(storage_connection_name, "insert_bucket")

        sql = fR'''
            INSERT INTO `{storage_connection_name}`.`test/` WITH (
                FORMAT = "{format}",
                COMPRESSION = "lz4"
            ) SELECT "hello" AS data;
            '''

        query = client.execute(sql)
        query.check(False)
        describe_result = query
        logging.debug("Describe result: {}".format(describe_result))
        describe_string = "{}".format(describe_result)
        assert "Compression 'lz4' is not supported for format '" + format + "'" in describe_string

    def test_invalid_compression_inference(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("fbucket")
        bucket.create(ACL='public-read')

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        fruits = R'''[{"name" : "banana", "price" : 3, "weight" : 100}]'''
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='fruits.json', ContentType='text/plain')

        storage_connection_name = client.path(unique_prefix + "fruitbucket")
        client.create_external_data_source(storage_connection_name, "fbucket")

        sql = fR'''
            SELECT *
            FROM `{storage_connection_name}`.`fruits.json`
            WITH (format=csv_with_names, compression="gzip", with_infer="true");
            '''

        query = client.execute(sql)
        query.check(False)
        describe_result = query
        logging.debug("Describe result: {}".format(describe_result))
        describe_string = "{}".format(describe_result)
        assert "couldn't decompress file, check compression params:" in describe_string
