#!/usr/bin/env python
# -*- coding: utf-8 -*-

import boto3
import logging

import pytest

import ydb.public.api.protos.ydb_value_pb2 as ydb
import ydb.public.api.protos.draft.fq_pb2 as fq

import ydb.tests.fq.s3.s3_helpers as s3_helpers
from ydb.tests.tools.fq_runner.kikimr_utils import yq_all


class TestS3Compressions:
    def create_bucket_and_upload_file(self, filename, s3, kikimr):
        s3_helpers.create_bucket_and_upload_file(filename, s3.s3_url, "fbucket", "ydb/tests/fq/s3/test_compression_data")
        kikimr.control_plane.wait_bootstrap(1)

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
        assert result_set.rows[0].items[0].bytes_value == b"yq"
        assert result_set.rows[0].items[1].int32_value == 0
        assert result_set.rows[0].items[2].bytes_value == b"abc"

    @yq_all
    @pytest.mark.parametrize("filename, compression", [
        ("test.json.gz", "gzip"),
        ("test.json.lz4", "lz4"),
        ("test.json.br", "brotli"),
        ("test.json.bz2", "bzip2"),
        ("test.json.zst", "zstd"),
        ("test.json.xz", "xz")
    ])
    def test_compression(self, kikimr, s3, client, filename, compression, unique_prefix):
        self.create_bucket_and_upload_file(filename, s3, kikimr)
        storage_connection_name = unique_prefix + "fruitbucket"
        client.create_storage_connection(storage_connection_name, "fbucket")

        sql = '''
            SELECT *
            FROM `{}`.`{}`
            WITH (format=json_each_row, compression="{}", SCHEMA (
                id Int32 NOT NULL,
                description String NOT NULL,
                info String NOT NULL
            ));
            '''.format(storage_connection_name, filename, compression)

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        self.validate_result(result_set)

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_invalid_compression(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("fbucket")
        bucket.create(ACL='public-read')

        s3_client = boto3.client(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        fruits = R'''Fruit,Price,Weight
Banana,3,100
Apple,2,22
Pear,15,33'''
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='fruits.csv', ContentType='text/plain')
        kikimr.control_plane.wait_bootstrap(1)

        storage_connection_name = unique_prefix + "fruitbucket"
        client.create_storage_connection(storage_connection_name, "fbucket")

        sql = fR'''
            SELECT *
            FROM `{storage_connection_name}`.`fruits.csv`
            WITH (format=csv_with_names, compression="some_compression", SCHEMA (
                Fruit String,
                Price Int,
                Weight Int
            ));
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.FAILED)
        describe_result = client.describe_query(query_id).result
        logging.debug("Describe result: {}".format(describe_result))
        describe_string = "{}".format(describe_result)
        assert "Unknown compression: some_compression. Use one of: gzip, zstd, lz4, brotli, bzip2, xz" in describe_string
