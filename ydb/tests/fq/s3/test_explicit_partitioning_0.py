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
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    @pytest.mark.parametrize("runtime_listing", ["false", "true"])
    def test_partitioned_by(self, kikimr, s3, client, runtime_listing, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("fbucket")
        bucket.create(ACL='public-read')

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        fruits = R'''Fruit,Price,Duration
Banana,3,100
Apple,2,22
Pear,15,33'''
        s3_client.put_object(
            Body=fruits,
            Bucket='fbucket',
            Key='hive_format/year=2022/month=3/day=5/fruits.csv',
            ContentType='text/plain',
        )
        s3_client.put_object(
            Body=fruits,
            Bucket='fbucket',
            Key='hive_format/year=2022/month=3/day=5/fruits.txt',
            ContentType='text/plain',
        )

        kikimr.control_plane.wait_bootstrap(1)
        connection_response = client.create_storage_connection(unique_prefix + "fruitbucket", "fbucket")

        yearType = ydb.Column(name="year", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT32))
        monthType = ydb.Column(name="month", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING))
        dayType = ydb.Column(name="day", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING))
        fruitType = ydb.Column(name="Fruit", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING))
        priceType = ydb.Column(name="Price", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT32))
        intervalType = ydb.Column(name="Duration", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT64))
        storage_binding_name = unique_prefix + "my_binding"
        client.create_object_storage_binding(
            name=storage_binding_name,
            path="hive_format",
            format="csv_with_names",
            connection_id=connection_response.result.connection_id,
            columns=[yearType, monthType, dayType, fruitType, priceType, intervalType],
            partitioned_by=["year", "month", "day"],
            format_setting={"file_pattern": "*t?.csv"},
        )

        sql = f'''
            pragma s3.UseRuntimeListing="{runtime_listing}";

            SELECT *
            FROM bindings.`{storage_binding_name}`;
            '''

        query_id = client.create_query("simple", sql).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        logging.debug(str(result_set))
        assert len(result_set.columns) == 6
        assert result_set.columns[0].name == "Duration"
        assert result_set.columns[0].type.type_id == ydb.Type.INT64
        assert result_set.columns[1].name == "Fruit"
        assert result_set.columns[1].type.type_id == ydb.Type.STRING
        assert result_set.columns[2].name == "Price"
        assert result_set.columns[2].type.type_id == ydb.Type.INT32
        assert result_set.columns[3].name == "day"
        assert result_set.columns[3].type.type_id == ydb.Type.STRING
        assert result_set.columns[4].name == "month"
        assert result_set.columns[4].type.type_id == ydb.Type.STRING
        assert result_set.columns[5].name == "year"
        assert result_set.columns[5].type.type_id == ydb.Type.INT32
        assert len(result_set.rows) == 3
        assert result_set.rows[0].items[0].int64_value == 100
        assert result_set.rows[0].items[1].bytes_value == b"Banana"
        assert result_set.rows[0].items[2].int32_value == 3
        assert result_set.rows[0].items[3].bytes_value == b"5"
        assert result_set.rows[0].items[4].bytes_value == b"3"
        assert result_set.rows[0].items[5].int32_value == 2022

        assert result_set.rows[1].items[0].int64_value == 22
        assert result_set.rows[1].items[1].bytes_value == b"Apple"
        assert result_set.rows[1].items[2].int32_value == 2
        assert result_set.rows[1].items[3].bytes_value == b"5"
        assert result_set.rows[1].items[4].bytes_value == b"3"
        assert result_set.rows[1].items[5].int32_value == 2022
        assert result_set.rows[2].items[0].int64_value == 33
        assert result_set.rows[2].items[1].bytes_value == b"Pear"
        assert result_set.rows[2].items[2].int32_value == 15
        assert result_set.rows[2].items[3].bytes_value == b"5"
        assert result_set.rows[2].items[4].bytes_value == b"3"
        assert result_set.rows[2].items[5].int32_value == 2022

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    @pytest.mark.parametrize("runtime_listing", ["false", "true"])
    def test_projection(self, kikimr, s3, client, runtime_listing, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("test_projection")
        bucket.create(ACL='public-read')

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        fruits = R'''Fruit,Price,Duration
Banana,3,100'''
        s3_client.put_object(Body=fruits, Bucket='test_projection', Key='2022/3/5/fruits.csv', ContentType='text/plain')
        s3_client.put_object(Body=fruits, Bucket='test_projection', Key='2022/3/6/fruits.csv', ContentType='text/plain')
        s3_client.put_object(Body=fruits, Bucket='test_projection', Key='2022/3/7/fruits.csv', ContentType='text/plain')
        s3_client.put_object(Body=fruits, Bucket='test_projection', Key='2022/3/8/fruits.csv', ContentType='text/plain')
        kikimr.control_plane.wait_bootstrap(1)
        connection_response = client.create_storage_connection(unique_prefix + "fruitbucket", "test_projection")

        yearType = ydb.Column(name="year", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING))
        monthType = ydb.Column(name="month", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING))
        dayType = ydb.Column(name="day", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING))
        fruitType = ydb.Column(name="Fruit", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING))
        priceType = ydb.Column(name="Price", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT32))
        intervalType = ydb.Column(name="Duration", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT64))
        storage_binding_name = unique_prefix + "my_binding"
        client.create_object_storage_binding(
            name=storage_binding_name,
            path="/",
            format="csv_with_names",
            connection_id=connection_response.result.connection_id,
            columns=[yearType, monthType, dayType, fruitType, priceType, intervalType],
            projection={
                "projection.enabled": "true",
                "projection.year.type": "enum",
                "projection.year.values": "2022",
                "projection.month.type": "enum",
                "projection.month.values": "3",
                "projection.day.type": "enum",
                "projection.day.values": "5,6,7,8",
                "storage.location.template": "${year}/${month}/${day}",
            },
            partitioned_by=["year", "month", "day"],
        )

        sql = f'''
            pragma s3.UseRuntimeListing="{runtime_listing}";

            SELECT *
            FROM bindings.`{storage_binding_name}`;
            '''

        query_id = client.create_query("simple", sql).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        logging.debug(str(result_set))
        assert len(result_set.columns) == 6
        assert result_set.columns[0].name == "Duration"
        assert result_set.columns[0].type.type_id == ydb.Type.INT64
        assert result_set.columns[1].name == "Fruit"
        assert result_set.columns[1].type.type_id == ydb.Type.STRING
        assert result_set.columns[2].name == "Price"
        assert result_set.columns[2].type.type_id == ydb.Type.INT32
        assert result_set.columns[3].name == "day"
        assert result_set.columns[3].type.type_id == ydb.Type.STRING
        assert result_set.columns[4].name == "month"
        assert result_set.columns[4].type.type_id == ydb.Type.STRING
        assert result_set.columns[5].name == "year"
        assert result_set.columns[5].type.type_id == ydb.Type.STRING
        assert len(result_set.rows) == 4
        assert result_set.rows[0].items[0].int64_value == 100
        assert result_set.rows[0].items[1].bytes_value == b"Banana"
        assert result_set.rows[0].items[2].int32_value == 3
        assert result_set.rows[0].items[4].bytes_value == b"3"
        assert result_set.rows[0].items[5].bytes_value == b"2022"

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    @pytest.mark.parametrize("runtime_listing", ["false", "true"])
    def test_pruning(self, kikimr, s3, client, runtime_listing, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("pbucket")
        bucket.create(ACL='public-read')

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        fruits = R'''Fruit,Price,Duration
Banana,3,100'''
        s3_client.put_object(
            Body=fruits,
            Bucket='pbucket',
            Key='hive_format/year=2022/month=3/day=5/fruits.csv',
            ContentType='text/plain',
        )
        s3_client.put_object(
            Body=fruits,
            Bucket='pbucket',
            Key='hive_format/year=2022/month=3/day=5/fruits.txt',
            ContentType='text/plain',
        )
        fruits2 = R'''Fruit,Price,Duration
Apple,2,22'''
        s3_client.put_object(
            Body=fruits2,
            Bucket='pbucket',
            Key='hive_format/year=2021/month=3/day=5/fruits2.csv',
            ContentType='text/plain',
        )
        s3_client.put_object(
            Body=fruits2,
            Bucket='pbucket',
            Key='hive_format/year=2021/month=3/day=5/fruits2.txt',
            ContentType='text/plain',
        )
        fruits3 = R'''invalid data'''
        s3_client.put_object(
            Body=fruits3,
            Bucket='pbucket',
            Key='hive_format/year=2020/month=3/day=5/fruits3.csv',
            ContentType='text/plain',
        )
        s3_client.put_object(
            Body=fruits3,
            Bucket='pbucket',
            Key='hive_format/year=2020/month=3/day=5/fruits3.txt',
            ContentType='text/plain',
        )
        kikimr.control_plane.wait_bootstrap(1)
        connection_response = client.create_storage_connection(unique_prefix + "fruitbucket", "pbucket")

        yearType = ydb.Column(name="year", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT32))
        monthType = ydb.Column(name="month", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING))
        dayType = ydb.Column(name="day", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING))
        fruitType = ydb.Column(name="Fruit", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING))
        priceType = ydb.Column(name="Price", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT32))
        intervalType = ydb.Column(name="Duration", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT64))
        storage_binding_name = unique_prefix + "my_binding"
        client.create_object_storage_binding(
            name=storage_binding_name,
            path="hive_format",
            format="csv_with_names",
            connection_id=connection_response.result.connection_id,
            columns=[yearType, monthType, dayType, fruitType, priceType, intervalType],
            partitioned_by=["year", "month", "day"],
            format_setting={"file_pattern": "*.csv"},
        )

        sql = f'''
            pragma s3.UseRuntimeListing="{runtime_listing}";

            SELECT *
            FROM bindings.`{storage_binding_name}` where year > 2020 order by Fruit;
            '''

        query_id = client.create_query("simple", sql).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        describe_result = client.describe_query(query_id).result
        assert "fruits3.csv" not in str(describe_result.query.ast)
        assert "fruits2.csv" in str(describe_result.query.ast)
        assert "fruits.csv" in str(describe_result.query.ast)

        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        logging.debug(str(result_set))
        assert len(result_set.columns) == 6
        assert result_set.columns[0].name == "Duration"
        assert result_set.columns[0].type.type_id == ydb.Type.INT64
        assert result_set.columns[1].name == "Fruit"
        assert result_set.columns[1].type.type_id == ydb.Type.STRING
        assert result_set.columns[2].name == "Price"
        assert result_set.columns[2].type.type_id == ydb.Type.INT32
        assert result_set.columns[3].name == "day"
        assert result_set.columns[3].type.type_id == ydb.Type.STRING
        assert result_set.columns[4].name == "month"
        assert result_set.columns[4].type.type_id == ydb.Type.STRING
        assert result_set.columns[5].name == "year"
        assert result_set.columns[5].type.type_id == ydb.Type.INT32
        assert len(result_set.rows) == 2

        assert result_set.rows[0].items[0].int64_value == 22
        assert result_set.rows[0].items[1].bytes_value == b"Apple"
        assert result_set.rows[0].items[2].int32_value == 2
        assert result_set.rows[0].items[3].bytes_value == b"5"
        assert result_set.rows[0].items[4].bytes_value == b"3"
        assert result_set.rows[0].items[5].int32_value == 2021

        assert result_set.rows[1].items[0].int64_value == 100
        assert result_set.rows[1].items[1].bytes_value == b"Banana"
        assert result_set.rows[1].items[2].int32_value == 3
        assert result_set.rows[1].items[3].bytes_value == b"5"
        assert result_set.rows[1].items[4].bytes_value == b"3"
        assert result_set.rows[1].items[5].int32_value == 2022

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_validation(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("fbucket")
        bucket.create(ACL='public-read')

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        fruits = R'''Fruit,Price,Duration
Banana,3,100
Apple,2,22
Pear,15,33'''
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='2022/3/5/fruits.csv', ContentType='text/plain')
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='2022/3/6/fruits.csv', ContentType='text/plain')
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='2022/3/7/fruits.csv', ContentType='text/plain')
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='2022/3/8/fruits.csv', ContentType='text/plain')
        kikimr.control_plane.wait_bootstrap(1)
        connection_response = client.create_storage_connection(unique_prefix + "fruitbucket", "fbucket")

        yearType = ydb.Column(name="year", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT32))
        monthType = ydb.Column(name="month", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING))
        dayType = ydb.Column(name="day", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING))
        fruitType = ydb.Column(name="Fruit", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING))
        priceType = ydb.Column(name="Price", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT32))
        intervalType = ydb.Column(name="Duration", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT64))
        binding_response = client.create_object_storage_binding(
            name=unique_prefix + "my_binding",
            path="hive_format",
            format="csv_with_names",
            connection_id=connection_response.result.connection_id,
            columns=[yearType, monthType, dayType, fruitType, priceType, intervalType],
            projection={"projection.enab": "true", "storage.location.template": "/${year}/${month}/${day}/"},
            partitioned_by=["year", "month", "day"],
            check_issues=False,
        )
        assert "Unknown key projection.enab" in str(binding_response.issues)

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    @pytest.mark.parametrize("runtime_listing", ["false", "true"])
    def test_no_schema_columns_except_partitioning_ones(self, kikimr, s3, client, runtime_listing, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("json_bucket")
        bucket.create(ACL='public-read')

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        json_body = "{}"
        s3_client.put_object(Body=json_body, Bucket='json_bucket', Key='2022/03/obj.json', ContentType='text/json')

        kikimr.control_plane.wait_bootstrap(1)
        storage_connection_name = unique_prefix + "json_bucket"
        client.create_storage_connection(storage_connection_name, "json_bucket")

        sql = (
            f'''
            pragma s3.UseRuntimeListing="{runtime_listing}";
            '''
            + R'''
            $projection =
            @@
            {
                "projection.enabled" : true,

                "projection.year.type" : "integer",
                "projection.year.min" : 2010,
                "projection.year.max" : 2022,
                "projection.year.interval" : 1,

                "projection.month.type" : "integer",
                "projection.month.min" : 1,
                "projection.month.max" : 12,
                "projection.month.interval" : 1,
                "projection.month.digits" : 2,

                "storage.location.template" : "${year}/${month}"
            }
            @@;
            '''
            + fR'''
            SELECT
                *
            FROM
                `{storage_connection_name}`.`/`
            WITH
            (
                format=json_as_string,
                schema=
                (
                    year Int,
                    month Int
                ),
                partitioned_by=(year, month),
                projection=$projection
            )
        '''
        )

        query_id = client.create_query("simple", sql).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.FAILED)

        describe_result = client.describe_query(query_id).result
        logging.info("AST: {}".format(describe_result.query.ast.data))
        describe_string = "{}".format(describe_result)
        assert "Table contains no columns except partitioning columns" in describe_string, describe_string

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    @pytest.mark.parametrize("runtime_listing", ["false", "true"])
    def test_projection_date(self, kikimr, s3, client, runtime_listing, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("test_projection_date")
        bucket.create(ACL='public-read')

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        fruits = R'''Fruit,Price,Duration
Banana,3,100'''
        s3_client.put_object(
            Body=fruits, Bucket='test_projection_date', Key='2022-03-05/fruits.csv', ContentType='text/plain'
        )
        kikimr.control_plane.wait_bootstrap(1)
        connection_response = client.create_storage_connection(unique_prefix + "fruitbucket", "test_projection_date")

        dt = ydb.Column(name="dt", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.DATE))
        fruitType = ydb.Column(name="Fruit", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING))
        priceType = ydb.Column(name="Price", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT32))
        intervalType = ydb.Column(name="Duration", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT64))
        storage_binding_name = unique_prefix + "my_binding"
        client.create_object_storage_binding(
            name=storage_binding_name,
            path="/",
            format="csv_with_names",
            connection_id=connection_response.result.connection_id,
            columns=[dt, fruitType, priceType, intervalType],
            projection={
                "projection.enabled": "true",
                "projection.dt.type": "date",
                "projection.dt.min": "2022-03-05",
                "projection.dt.max": "2022-03-05",
                "projection.dt.format": "%F",
                "projection.dt.unit": "YEARS",
                "storage.location.template": "${dt}",
            },
            partitioned_by=["dt"],
        )

        sql = f'''
            pragma s3.UseRuntimeListing="{runtime_listing}";

            SELECT *
            FROM bindings.`{storage_binding_name}`;
            '''

        query_id = client.create_query("simple", sql).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        logging.debug(str(result_set))
        assert len(result_set.columns) == 4
        assert result_set.columns[0].name == "Duration"
        assert result_set.columns[0].type.type_id == ydb.Type.INT64
        assert result_set.columns[1].name == "Fruit"
        assert result_set.columns[1].type.type_id == ydb.Type.STRING
        assert result_set.columns[2].name == "Price"
        assert result_set.columns[2].type.type_id == ydb.Type.INT32
        assert result_set.columns[3].name == "dt"
        assert result_set.columns[3].type.type_id == ydb.Type.DATE
        assert len(result_set.rows) == 1
        assert result_set.rows[0].items[0].int64_value == 100
        assert result_set.rows[0].items[1].bytes_value == b"Banana"
        assert result_set.rows[0].items[2].int32_value == 3
        assert result_set.rows[0].items[3].uint32_value == 19056

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_projection_validate_columns(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("test_projection_validate_columns")
        bucket.create(ACL='public-read')

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        fruits = R'''Fruit,Price,Duration
Banana,3,100'''
        s3_client.put_object(
            Body=fruits,
            Bucket='test_projection_validate_columns',
            Key='2022-03-05/fruits.csv',
            ContentType='text/plain',
        )
        kikimr.control_plane.wait_bootstrap(1)
        connection_response = client.create_storage_connection(
            unique_prefix + "fruitbucket", "test_projection_validate_columns"
        )

        dt = ydb.Column(name="dt", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.DATE))
        fruitType = ydb.Column(name="Fruit", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING))
        priceType = ydb.Column(name="Price", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT32))
        intervalType = ydb.Column(name="Duration", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT64))
        binding_response = client.create_object_storage_binding(
            name=unique_prefix + "my_binding",
            path="/",
            format="csv_with_names",
            connection_id=connection_response.result.connection_id,
            columns=[dt, fruitType, priceType, intervalType],
            projection={"projection.enabled": "true", "storage.location.template": "${dt}"},
            partitioned_by=["dt"],
            check_issues=False,
        )
        assert "Projection column named dt does not exist for template ${dt}/" in str(binding_response.issues)

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    @pytest.mark.parametrize("runtime_listing", ["false", "true"])
    def test_no_paritioning_columns(self, kikimr, s3, client, runtime_listing, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("logs2")
        bucket.create(ACL='public-read')

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        json_body = '''
        {
            "@timestamp":"2023-01-15T22:07:48.802986Z",
            "microseconds":1673820468802986,
            "host":"yq-red-beta-13",
            "cluster":"",
            "priority":"DEBUG",
            "npriority":7,
            "component":"INTERCONNECT",
            "tag":"KIKIMR",
            "revision":-1,
            "message":
            "Handshake [10013:7189004169974939133:4884] [node 0] ICH02 starting incoming handshake"
        }
        {
            "@timestamp":"2023-01-15T22:07:48.918225Z",
            "microseconds":1673820468918225,
            "host":"yq-cp-4",
            "cluster":"",
            "priority":"DEBUG",
            "npriority":7,
            "component":"YQL_PRIVATE_PROXY",
            "tag":"KIKIMR",
            "revision":-1,
            "message":"PrivateGetTask - Owner: 567b6222-32b12b4a-5935e5fb-d937c12b376053, Host: yq-red-alpha-22, Tenant: root-alpha, Got CP::GetTask Response"
        }
        '''
        s3_client.put_object(
            Body=json_body, Bucket='logs2', Key='logs/year=2023/month=01/day=16/obj1.json', ContentType='text/json'
        )
        s3_client.put_object(
            Body=json_body, Bucket='logs2', Key='logs/year=2023/month=01/day=16/obj2.json', ContentType='text/json'
        )

        kikimr.control_plane.wait_bootstrap(1)
        storage_connection_name = unique_prefix + "logs2"
        client.create_storage_connection(storage_connection_name, "logs2")

        sql = (
            f'''
            pragma s3.UseRuntimeListing="{runtime_listing}";
            '''
            + R'''
            $projection =
            @@ {
                "projection.enabled" : "true",
                "storage.location.template" : "/${date}",
                "projection.date.type" : "date",
                "projection.date.min" : "2023-01-16",
                "projection.date.max" : "2023-01-16",
                "projection.date.interval" : "1",
                "projection.date.format" : "year=%Y/month=%m/day=%d",
                "projection.date.unit" : "DAYS"
            }
            @@;
            '''
            + fR'''
            SELECT
                count(message)
            FROM
                `{storage_connection_name}`.`/logs`
            WITH (FORMAT="json_each_row",
                SCHEMA=(
                `@timestamp` String,
                `cluster` String,
                `component` String,
                `host` String,
                `message` String,
                `microseconds` Uint64,
                `npriority` Uint32,
                `priority` String,
                `revision` Int32,
                `tag` String,
                `date` Date NOT NULL
                ),
                partitioned_by=(`date`),
                projection=$projection)
            LIMIT 10;
        '''
        )

        query_id = client.create_query("simple", sql).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        describe_result = client.describe_query(query_id).result
        logging.info("AST: {}".format(describe_result.query.ast.data))

        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        logging.debug(str(result_set))
        assert len(result_set.columns) == 1
        assert result_set.columns[0].name == "column0"
        assert result_set.columns[0].type.type_id == ydb.Type.UINT64
        assert len(result_set.rows) == 1
        assert result_set.rows[0].items[0].uint64_value == 4

    @yq_all
    @pytest.mark.parametrize(
        "client, column_type, is_correct",
        [
            ({"folder_id": "my_folder1"}, "year Int32 NOT NULL", True),
            ({"folder_id": "my_folder2"}, "year Uint32 NOT NULL", True),
            ({"folder_id": "my_folder3"}, "year Uint64 NOT NULL", True),
            ({"folder_id": "my_folder4"}, "year Date NOT NULL", False),
            ({"folder_id": "my_folder5"}, "year String NOT NULL", True),
            ({"folder_id": "my_folder6"}, "year String", False),
            ({"folder_id": "my_folder7"}, "year Utf8 NOT NULL", True),
            ({"folder_id": "my_folder8"}, "year Utf8", False),
            ({"folder_id": "my_folder9"}, "year Int32", False),
            ({"folder_id": "my_folder10"}, "year Uint32", False),
            ({"folder_id": "my_folder11"}, "year Int64 NOT NULL", True),
            ({"folder_id": "my_folder12"}, "year Int64", False),
            ({"folder_id": "my_folder13"}, "year Uint64", False),
            ({"folder_id": "my_folder14"}, "year Date", False),
        ],
        indirect=["client"],
    )
    @pytest.mark.parametrize("runtime_listing", ["false", "true"])
    def test_projection_integer_type_validation(
        self, kikimr, s3, client, column_type, is_correct, runtime_listing, unique_prefix
    ):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("test_projection_integer_type_validation")
        bucket.create(ACL='public-read')

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        fruits = R'''Fruit,Price,Duration
Banana,3,100'''
        s3_client.put_object(
            Body=fruits,
            Bucket='test_projection_integer_type_validation',
            Key='2022-03-05/fruits.csv',
            ContentType='text/plain',
        )
        kikimr.control_plane.wait_bootstrap(1)
        storage_connection_name = unique_prefix + "fruitbucket"
        client.create_storage_connection(storage_connection_name, "test_projection_integer_type_validation")

        sql = (
            f'''
            pragma s3.UseRuntimeListing="{runtime_listing}";
            '''
            + R'''
            $projection =
            @@
            {
                "projection.enabled" : true,

                "projection.year.type" : "integer",
                "projection.year.min" : 2022,
                "projection.year.max" : 2022,
                "projection.year.interval" : 1,

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
            ({"folder_id": "my_folder1"}, "year Int32 NOT NULL", False),
            ({"folder_id": "my_folder2"}, "year Uint32 NOT NULL", False),
            ({"folder_id": "my_folder3"}, "year Uint64 NOT NULL", False),
            ({"folder_id": "my_folder4"}, "year Date NOT NULL", False),
            ({"folder_id": "my_folder5"}, "year Utf8 NOT NULL", False),
            ({"folder_id": "my_folder6"}, "year Int64 NOT NULL", False),
            ({"folder_id": "my_folder1"}, "year Int32", False),
            ({"folder_id": "my_folder2"}, "year Uint32", False),
            ({"folder_id": "my_folder3"}, "year Int64", False),
            ({"folder_id": "my_folder4"}, "year Uint64", False),
            ({"folder_id": "my_folder6"}, "year String NOT NULL", True),
            ({"folder_id": "my_folder7"}, "year String", False),
            ({"folder_id": "my_folder8"}, "year Utf8", False),
            ({"folder_id": "my_folder9"}, "year Date", False),
        ],
        indirect=["client"],
    )
    @pytest.mark.parametrize("runtime_listing", ["false", "true"])
    def test_projection_enum_type_invalid_validation(
        self, kikimr, s3, client, column_type, is_correct, runtime_listing, unique_prefix
    ):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("test_projection_enum_type_invalid_validation")
        bucket.create(ACL='public-read')

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        fruits = R'''Fruit,Price,Duration
Banana,3,100'''
        s3_client.put_object(
            Body=fruits,
            Bucket='test_projection_enum_type_invalid_validation',
            Key='2022-03-05/fruits.csv',
            ContentType='text/plain',
        )
        kikimr.control_plane.wait_bootstrap(1)
        storage_connection_name = unique_prefix + "fruitbucket"
        client.create_storage_connection(storage_connection_name, "test_projection_enum_type_invalid_validation")

        sql = (
            f'''
            pragma s3.UseRuntimeListing="{runtime_listing}";
            '''
            + R'''
            $projection =
            @@
            {
                "projection.enabled" : true,

                "projection.year.type" : "enum",
                "projection.year.values" : "2022",

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
