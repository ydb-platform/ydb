#!/usr/bin/env python
# -*- coding: utf-8 -*-

import boto3
import logging

import pytest

from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase

import ydb.public.api.protos.ydb_value_pb2 as ydb
import ydb.public.api.protos.draft.fq_pb2 as fq
from ydb.tests.tools.fq_runner.kikimr_utils import yq_all


class TestS3(TestYdsBase):
    @yq_all
    @pytest.mark.parametrize(
        "client, column_type, is_correct",
        [
            ({"folder_id": "my_folder1"}, "year Int32", False),
            ({"folder_id": "my_folder2"}, "year Int32 NOT NULL", False),
            ({"folder_id": "my_folder3"}, "year Uint32", False),
            ({"folder_id": "my_folder4"}, "year Uint32 NOT NULL", True),
            ({"folder_id": "my_folder5"}, "year Int64", False),
            ({"folder_id": "my_folder6"}, "year Int64 NOT NULL", False),
            ({"folder_id": "my_folder7"}, "year Uint64", False),
            ({"folder_id": "my_folder8"}, "year Uint64 NOT NULL", False),
            ({"folder_id": "my_folder9"}, "year String NOT NULL", True),
            ({"folder_id": "my_folder10"}, "year String", False),
            ({"folder_id": "my_folder11"}, "year Utf8", False),
            ({"folder_id": "my_folder12"}, "year Utf8 NOT NULL", True),
            ({"folder_id": "my_folder13"}, "year Date", False),
            ({"folder_id": "my_folder14"}, "year Date NOT NULL", True),
            ({"folder_id": "my_folder15"}, "year Datetime", False),
            ({"folder_id": "my_folder16"}, "year Datetime NOT NULL", True),
        ],
        indirect=["client"],
    )
    @pytest.mark.parametrize("runtime_listing", ["false", "true"])
    def test_projection_date_type_validation(
        self, kikimr, s3, client, column_type, is_correct, runtime_listing, unique_prefix
    ):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("test_projection_date_type_invalid_validation")
        bucket.create(ACL='public-read')

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        fruits = R'''Fruit,Price,Duration
Banana,3,100'''
        s3_client.put_object(
            Body=fruits,
            Bucket='test_projection_date_type_invalid_validation',
            Key='2022-03-05/fruits.csv',
            ContentType='text/plain',
        )
        kikimr.control_plane.wait_bootstrap(1)
        storage_connection_name = unique_prefix + "fruitbucket"
        client.create_storage_connection(storage_connection_name, "test_projection_date_type_invalid_validation")

        sql = (
            f'''
            pragma s3.UseRuntimeListing="{runtime_listing}";
            '''
            + R'''
            $projection =
            @@
            {
                "projection.enabled" : true,

                "projection.year.type" : "date",
                "projection.year.min" : "2022-01-01",
                "projection.year.max" : "2022-01-01",
                "projection.year.format" : "%Y",
                "projection.year.interval" : "1",
                "projection.year.unit" : "DAYS",

                "storage.location.template" : "${year}-03-05"
            }
            @@;
            '''
            + f'''
            SELECT *
            FROM `{storage_connection_name}`.`/` WITH
            (
                format=csv_with_names,
                schema=
                (
                    Fruit String NOT NULL,
                    {column_type}
                ),
                partitioned_by=(year),
                projection=$projection
            )
            '''
        )

        query_id = client.create_query("simple", sql).result.query_id
        if is_correct:
            client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
            data = client.get_result_data(query_id)
            result_set = data.result.result_set
            logging.debug(str(result_set))
            assert len(result_set.columns) == 2
            assert result_set.columns[0].name == "Fruit"
            assert result_set.columns[0].type.type_id == ydb.Type.STRING
            assert len(result_set.rows) == 1
            assert result_set.rows[0].items[0].bytes_value == b"Banana"
        else:
            client.wait_query_status(query_id, fq.QueryMeta.FAILED)
            describe_result = client.describe_query(query_id).result
            logging.info("AST: {}".format(describe_result.query.ast.data))
            assert "Projection column \\\"year\\\" has invalid type" in str(describe_result.query.issue), str(
                describe_result.query.issue
            )

    @yq_all
    @pytest.mark.parametrize(
        "client, column_type, is_correct",
        [
            ({"folder_id": "my_folder1"}, ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT32), True),
            ({"folder_id": "my_folder2"}, ydb.Type(type_id=ydb.Type.PrimitiveTypeId.UINT32), True),
            ({"folder_id": "my_folder3"}, ydb.Type(type_id=ydb.Type.PrimitiveTypeId.UINT64), True),
            ({"folder_id": "my_folder4"}, ydb.Type(type_id=ydb.Type.PrimitiveTypeId.DATE), False),
            ({"folder_id": "my_folder5"}, ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT64), True),
            ({"folder_id": "my_folder6"}, ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING), True),
            ({"folder_id": "my_folder7"}, ydb.Type(type_id=ydb.Type.PrimitiveTypeId.UTF8), True),
            (
                {"folder_id": "my_folder8"},
                ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT32))),
                False,
            ),
            (
                {"folder_id": "my_folder9"},
                ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.UINT32))),
                False,
            ),
            (
                {"folder_id": "my_folder10"},
                ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.UINT64))),
                False,
            ),
            (
                {"folder_id": "my_folder11"},
                ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.DATE))),
                False,
            ),
            (
                {"folder_id": "my_folder12"},
                ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT64))),
                False,
            ),
            (
                {"folder_id": "my_folder13"},
                ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING))),
                False,
            ),
            (
                {"folder_id": "my_folder14"},
                ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.UTF8))),
                False,
            ),
        ],
        indirect=["client"],
    )
    def test_binding_projection_integer_type_validation(
        self, kikimr, s3, client, column_type, is_correct, unique_prefix
    ):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("test_binding_projection_integer_type_validation")
        bucket.create(ACL='public-read')

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        fruits = R'''Fruit,Price,Duration
Banana,3,100'''
        s3_client.put_object(
            Body=fruits,
            Bucket='test_binding_projection_integer_type_validation',
            Key='2022-03-05/fruits.csv',
            ContentType='text/plain',
        )
        kikimr.control_plane.wait_bootstrap(1)
        connection_response = client.create_storage_connection(
            unique_prefix + "fruitbucket", "test_binding_projection_integer_type_validation"
        )

        year = ydb.Column(name="year", type=column_type)
        fruitType = ydb.Column(name="Fruit", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING))
        binding_response = client.create_object_storage_binding(
            name=unique_prefix + "my_binding",
            path="/",
            format="csv_with_names",
            connection_id=connection_response.result.connection_id,
            columns=[year, fruitType],
            projection={
                "projection.enabled": "true",
                "projection.year.type": "integer",
                "projection.year.min": "2022",
                "projection.year.max": "2022",
                "storage.location.template": "${year}-03-05",
            },
            partitioned_by=["year"],
            check_issues=is_correct,
        )
        if not is_correct:
            assert "Column \\\"year\\\" from projection does not support" in str(binding_response.issues)

    @yq_all
    @pytest.mark.parametrize(
        "client, column_type, is_correct",
        [
            ({"folder_id": "my_folder1"}, ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT32), False),
            ({"folder_id": "my_folder2"}, ydb.Type(type_id=ydb.Type.PrimitiveTypeId.UINT32), False),
            ({"folder_id": "my_folder3"}, ydb.Type(type_id=ydb.Type.PrimitiveTypeId.UINT64), False),
            ({"folder_id": "my_folder4"}, ydb.Type(type_id=ydb.Type.PrimitiveTypeId.DATE), False),
            ({"folder_id": "my_folder5"}, ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT64), False),
            ({"folder_id": "my_folder6"}, ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING), True),
            ({"folder_id": "my_folder7"}, ydb.Type(type_id=ydb.Type.PrimitiveTypeId.UTF8), False),
            (
                {"folder_id": "my_folder8"},
                ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT32))),
                False,
            ),
            (
                {"folder_id": "my_folder9"},
                ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.UINT32))),
                False,
            ),
            (
                {"folder_id": "my_folder10"},
                ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.UINT64))),
                False,
            ),
            (
                {"folder_id": "my_folder11"},
                ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.DATE))),
                False,
            ),
            (
                {"folder_id": "my_folder12"},
                ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT64))),
                False,
            ),
            (
                {"folder_id": "my_folder13"},
                ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING))),
                False,
            ),
            (
                {"folder_id": "my_folder14"},
                ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.UTF8))),
                False,
            ),
        ],
        indirect=["client"],
    )
    def test_binding_projection_enum_type_validation(self, kikimr, s3, client, column_type, is_correct, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("test_binding_projection_enum_type_validation")
        bucket.create(ACL='public-read')

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        fruits = R'''Fruit,Price,Duration
Banana,3,100'''
        s3_client.put_object(
            Body=fruits,
            Bucket='test_binding_projection_enum_type_validation',
            Key='2022-03-05/fruits.csv',
            ContentType='text/plain',
        )
        kikimr.control_plane.wait_bootstrap(1)
        connection_response = client.create_storage_connection(
            unique_prefix + "fruitbucket", "test_binding_projection_enum_type_validation"
        )

        year = ydb.Column(name="year", type=column_type)
        fruitType = ydb.Column(name="Fruit", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING))
        binding_response = client.create_object_storage_binding(
            name=unique_prefix + "my_binding",
            path="/",
            format="csv_with_names",
            connection_id=connection_response.result.connection_id,
            columns=[year, fruitType],
            projection={
                "projection.enabled": "true",
                "projection.year.type": "enum",
                "projection.year.values": "2022",
                "storage.location.template": "${year}-03-05",
            },
            partitioned_by=["year"],
            check_issues=is_correct,
        )
        if not is_correct:
            assert "Column \\\"year\\\" from projection does not support" in str(binding_response.issues)

    @yq_all
    @pytest.mark.parametrize(
        "client, column_type, is_correct",
        [
            ({"folder_id": "my_folder1"}, ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT32), False),
            ({"folder_id": "my_folder2"}, ydb.Type(type_id=ydb.Type.PrimitiveTypeId.UINT32), True),
            ({"folder_id": "my_folder3"}, ydb.Type(type_id=ydb.Type.PrimitiveTypeId.UINT64), False),
            ({"folder_id": "my_folder4"}, ydb.Type(type_id=ydb.Type.PrimitiveTypeId.DATE), True),
            ({"folder_id": "my_folder5"}, ydb.Type(type_id=ydb.Type.PrimitiveTypeId.DATETIME), True),
            ({"folder_id": "my_folder6"}, ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT64), False),
            ({"folder_id": "my_folder7"}, ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING), True),
            ({"folder_id": "my_folder8"}, ydb.Type(type_id=ydb.Type.PrimitiveTypeId.UTF8), True),
            (
                {"folder_id": "my_folder9"},
                ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT32))),
                False,
            ),
            (
                {"folder_id": "my_folder10"},
                ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.UINT32))),
                False,
            ),
            (
                {"folder_id": "my_folder11"},
                ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.UINT64))),
                False,
            ),
            (
                {"folder_id": "my_folder12"},
                ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.DATE))),
                False,
            ),
            (
                {"folder_id": "my_folder13"},
                ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.DATETIME))),
                False,
            ),
            (
                {"folder_id": "my_folder14"},
                ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT64))),
                False,
            ),
            (
                {"folder_id": "my_folder15"},
                ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING))),
                False,
            ),
            (
                {"folder_id": "my_folder16"},
                ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.UTF8))),
                False,
            ),
        ],
        indirect=["client"],
    )
    def test_binding_projection_date_type_validation(self, kikimr, s3, client, column_type, is_correct, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("test_binding_projection_date_type_validation")
        bucket.create(ACL='public-read')

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        fruits = R'''Fruit,Price,Duration
Banana,3,100'''
        s3_client.put_object(
            Body=fruits,
            Bucket='test_binding_projection_date_type_validation',
            Key='2022-03-05/fruits.csv',
            ContentType='text/plain',
        )
        kikimr.control_plane.wait_bootstrap(1)
        connection_response = client.create_storage_connection(
            unique_prefix + "fruitbucket", "test_binding_projection_date_type_validation"
        )

        year = ydb.Column(name="year", type=column_type)
        fruitType = ydb.Column(name="Fruit", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING))
        binding_response = client.create_object_storage_binding(
            name=unique_prefix + "my_binding",
            path="/",
            format="csv_with_names",
            connection_id=connection_response.result.connection_id,
            columns=[year, fruitType],
            projection={
                "projection.enabled": "true",
                "projection.year.type": "date",
                "projection.year.min": "2022-01-01",
                "projection.year.max": "2022-01-01",
                "projection.year.format": "%Y",
                "projection.year.interval": "1",
                "projection.year.unit": "DAYS",
                "storage.location.template": "${year}-03-05",
            },
            partitioned_by=["year"],
            check_issues=is_correct,
        )
        if not is_correct:
            assert "Column \\\"year\\\" from projection does not support" in str(binding_response.issues)

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    @pytest.mark.parametrize("runtime_listing", ["false", "true"])
    def test_raw_format(self, kikimr, s3, client, runtime_listing, yq_version, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("raw_bucket")
        bucket.create(ACL='public-read')

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        s3_client.put_object(
            Body='text',
            Bucket='raw_bucket',
            Key='raw_format/year=2023/month=01/day=14/file1.txt',
            ContentType='text/plain',
        )

        kikimr.control_plane.wait_bootstrap(1)
        storage_connection_name = unique_prefix + "rawbucket"
        client.create_storage_connection(storage_connection_name, "raw_bucket")

        sql = (
            f'''
            pragma s3.UseRuntimeListing="{runtime_listing}";
            '''
            + R'''
            $projection = @@ {
                "projection.enabled" : "true",
                "storage.location.template" : "/${timestamp}",
                "projection.timestamp.type" : "date",
                "projection.timestamp.min" : "2023-01-14",
                "projection.timestamp.max" : "2023-01-14",
                "projection.timestamp.interval" : "1",
                "projection.timestamp.format" : "year=%Y/month=%m/day=%d",
                "projection.timestamp.unit" : "DAYS"
            }
            @@;
            '''
            + fR'''
            SELECT
                data,
                timestamp
            FROM
                `{storage_connection_name}`.`raw_format`
            WITH
            (
                format="raw",
                schema=(
                    `data` String NOT NULL,
                    `timestamp` Date NOT NULL
                ),
                partitioned_by=(`timestamp`),
                projection=$projection
            )
        '''
        )

        # temporary fix for dynamic listing
        if yq_version == "v1":
            sql = 'pragma dq.MaxTasksPerStage="10"; ' + sql

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        describe_result = client.describe_query(query_id).result
        logging.info("AST: {}".format(describe_result.query.ast.data))

        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        logging.debug(str(result_set))

        assert len(result_set.columns) == 2
        assert result_set.columns[0].name == "data"
        assert result_set.columns[0].type.type_id == ydb.Type.STRING
        assert result_set.columns[1].name == "timestamp"
        assert result_set.columns[1].type.type_id == ydb.Type.DATE
        assert len(result_set.rows) == 1
        assert result_set.rows[0].items[0].bytes_value == b"text"
        assert result_set.rows[0].items[1].uint32_value == 19371

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    @pytest.mark.parametrize("runtime_listing", ["false", "true"])
    def test_parquet(self, kikimr, s3, client, runtime_listing, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("parquets")
        bucket.create(ACL='public-read-write')
        bucket.objects.all().delete()

        kikimr.control_plane.wait_bootstrap(1)
        storage_connection_name = unique_prefix + "pb"
        client.create_storage_connection(storage_connection_name, "parquets")

        sql = fR'''
            insert into `{storage_connection_name}`.`part/x=1/` with (format="parquet")
            select * from AS_TABLE([<|foo:123, bar:"xxx"u|>,<|foo:456, bar:"yyy"u|>]);
            '''
        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        sql = fR'''
            insert into `{storage_connection_name}`.`part/x=2/` with (format="parquet")
            select * from AS_TABLE([<|foo:234, bar:"zzz"u|>,<|foo:567, bar:"ttt"u|>]);
            '''
        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        sql = f'''
            pragma s3.UseRuntimeListing="{runtime_listing}";

            SELECT foo, bar, x FROM `{storage_connection_name}`.`part/`
            WITH
            (
                format="parquet",
                schema=(
                    foo Int NOT NULL,
                    bar String NOT NULL,
                    x Int NOT NULL,
                ),
                partitioned_by=(`x`)
            )
            ORDER BY foo
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        logging.debug(str(result_set))
        assert len(result_set.columns) == 3
        assert result_set.columns[0].name == "foo"
        print(str(result_set.columns[0]))
        assert result_set.columns[0].type.type_id == ydb.Type.INT32
        assert result_set.columns[1].name == "bar"
        assert result_set.columns[1].type.type_id == ydb.Type.STRING
        assert result_set.columns[2].name == "x"
        assert result_set.columns[2].type.type_id == ydb.Type.INT32
        assert len(result_set.rows) == 4
        assert result_set.rows[0].items[0].int32_value == 123
        assert result_set.rows[1].items[0].int32_value == 234
        assert result_set.rows[2].items[0].int32_value == 456
        assert result_set.rows[3].items[0].int32_value == 567
        assert result_set.rows[0].items[1].bytes_value == b"xxx"
        assert result_set.rows[1].items[1].bytes_value == b"zzz"
        assert result_set.rows[2].items[1].bytes_value == b"yyy"
        assert result_set.rows[3].items[1].bytes_value == b"ttt"
        assert result_set.rows[0].items[2].int32_value == 1
        assert result_set.rows[1].items[2].int32_value == 2
        assert result_set.rows[2].items[2].int32_value == 1
        assert result_set.rows[3].items[2].int32_value == 2
