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
    @pytest.mark.parametrize("runtime_listing", [False, True])
    def test_partitioned_by(self, kikimr, s3, client, runtime_listing, yq_version):
        if yq_version == 'v1' and runtime_listing:
            pytest.skip("Runtime listing is v2 only")

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

        fruits = R'''Fruit,Price,Duration
Banana,3,100
Apple,2,22
Pear,15,33'''
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='hive_format/year=2022/month=3/day=5/fruits.csv', ContentType='text/plain')
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='hive_format/year=2022/month=3/day=5/fruits.txt', ContentType='text/plain')
        kikimr.control_plane.wait_bootstrap(1)
        connection_response = client.create_storage_connection("fruitbucket", "fbucket")

        yearType = ydb.Column(name="year", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT32))
        monthType = ydb.Column(name="month", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING))
        dayType = ydb.Column(name="day", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING))
        fruitType = ydb.Column(name="Fruit", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING))
        priceType = ydb.Column(name="Price", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT32))
        intervalType = ydb.Column(name="Duration", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT64))
        client.create_object_storage_binding(name="my_binding",
                                             path="hive_format",
                                             format="csv_with_names",
                                             connection_id=connection_response.result.connection_id,
                                             columns=[yearType, monthType, dayType, fruitType, priceType, intervalType],
                                             partitioned_by=["year", "month", "day"],
                                             format_setting={
                                                 "file_pattern": "*t?.csv"
                                             })

        sql = f'''
            pragma s3.UseRuntimeListing="{str(runtime_listing).lower()}";

            SELECT *
            FROM bindings.my_binding;
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
    @pytest.mark.parametrize("runtime_listing", [False, True])
    def test_projection(self, kikimr, s3, client, runtime_listing, yq_version):
        if yq_version == "v1" and runtime_listing:
            pytest.skip("Runtime listing is v2 only")


        resource = boto3.resource(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("test_projection")
        bucket.create(ACL='public-read')

        s3_client = boto3.client(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        fruits = R'''Fruit,Price,Duration
Banana,3,100'''
        s3_client.put_object(Body=fruits, Bucket='test_projection', Key='2022/3/5/fruits.csv', ContentType='text/plain')
        s3_client.put_object(Body=fruits, Bucket='test_projection', Key='2022/3/6/fruits.csv', ContentType='text/plain')
        s3_client.put_object(Body=fruits, Bucket='test_projection', Key='2022/3/7/fruits.csv', ContentType='text/plain')
        s3_client.put_object(Body=fruits, Bucket='test_projection', Key='2022/3/8/fruits.csv', ContentType='text/plain')
        kikimr.control_plane.wait_bootstrap(1)
        connection_response = client.create_storage_connection("fruitbucket", "test_projection")

        yearType = ydb.Column(name="year", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING))
        monthType = ydb.Column(name="month", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING))
        dayType = ydb.Column(name="day", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING))
        fruitType = ydb.Column(name="Fruit", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING))
        priceType = ydb.Column(name="Price", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT32))
        intervalType = ydb.Column(name="Duration", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT64))
        client.create_object_storage_binding(name="my_binding",
                                             path="/",
                                             format="csv_with_names",
                                             connection_id=connection_response.result.connection_id,
                                             columns=[yearType, monthType, dayType, fruitType, priceType, intervalType],
                                             projection={
                                                 "projection.enabled": "true",
                                                 "projection.year.type" : "enum",
                                                 "projection.year.values" : "2022",
                                                 "projection.month.type" : "enum",
                                                 "projection.month.values" : "3",
                                                 "projection.day.type" : "enum",
                                                 "projection.day.values" : "5,6,7,8",
                                                 "storage.location.template": "${year}/${month}/${day}"
                                             },
                                             partitioned_by=["year", "month", "day"])

        sql = f'''
            pragma s3.UseRuntimeListing="{str(runtime_listing).lower()}";

            SELECT *
            FROM bindings.my_binding;
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
    @pytest.mark.parametrize("runtime_listing", [False, True])
    def test_pruning(self, kikimr, s3, client, runtime_listing, yq_version):
        if yq_version == "v1" and runtime_listing:
            pytest.skip("Runtime listing is v2 only")

        resource = boto3.resource(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("pbucket")
        bucket.create(ACL='public-read')

        s3_client = boto3.client(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        fruits = R'''Fruit,Price,Duration
Banana,3,100'''
        s3_client.put_object(Body=fruits, Bucket='pbucket', Key='hive_format/year=2022/month=3/day=5/fruits.csv', ContentType='text/plain')
        s3_client.put_object(Body=fruits, Bucket='pbucket', Key='hive_format/year=2022/month=3/day=5/fruits.txt', ContentType='text/plain')
        fruits2 = R'''Fruit,Price,Duration
Apple,2,22'''
        s3_client.put_object(Body=fruits2, Bucket='pbucket', Key='hive_format/year=2021/month=3/day=5/fruits2.csv', ContentType='text/plain')
        s3_client.put_object(Body=fruits2, Bucket='pbucket', Key='hive_format/year=2021/month=3/day=5/fruits2.txt', ContentType='text/plain')
        fruits3 = R'''invalid data'''
        s3_client.put_object(Body=fruits3, Bucket='pbucket', Key='hive_format/year=2020/month=3/day=5/fruits3.csv', ContentType='text/plain')
        s3_client.put_object(Body=fruits3, Bucket='pbucket', Key='hive_format/year=2020/month=3/day=5/fruits3.txt', ContentType='text/plain')
        kikimr.control_plane.wait_bootstrap(1)
        connection_response = client.create_storage_connection("fruitbucket", "pbucket")

        yearType = ydb.Column(name="year", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT32))
        monthType = ydb.Column(name="month", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING))
        dayType = ydb.Column(name="day", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING))
        fruitType = ydb.Column(name="Fruit", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING))
        priceType = ydb.Column(name="Price", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT32))
        intervalType = ydb.Column(name="Duration", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT64))
        client.create_object_storage_binding(name="my_binding",
                                             path="hive_format",
                                             format="csv_with_names",
                                             connection_id=connection_response.result.connection_id,
                                             columns=[yearType, monthType, dayType, fruitType, priceType, intervalType],
                                             partitioned_by=["year", "month", "day"],
                                             format_setting={
                                                 "file_pattern": "*.csv"
                                             })

        sql = f'''
            pragma s3.UseRuntimeListing="{str(runtime_listing).lower()}";

            SELECT *
            FROM bindings.my_binding where year > 2020 order by Fruit;
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
    def test_validation(self, kikimr, s3, client):
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

        fruits = R'''Fruit,Price,Duration
Banana,3,100
Apple,2,22
Pear,15,33'''
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='2022/3/5/fruits.csv', ContentType='text/plain')
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='2022/3/6/fruits.csv', ContentType='text/plain')
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='2022/3/7/fruits.csv', ContentType='text/plain')
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='2022/3/8/fruits.csv', ContentType='text/plain')
        kikimr.control_plane.wait_bootstrap(1)
        connection_response = client.create_storage_connection("fruitbucket", "fbucket")

        yearType = ydb.Column(name="year", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT32))
        monthType = ydb.Column(name="month", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING))
        dayType = ydb.Column(name="day", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING))
        fruitType = ydb.Column(name="Fruit", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING))
        priceType = ydb.Column(name="Price", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT32))
        intervalType = ydb.Column(name="Duration", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT64))
        binding_response = client.create_object_storage_binding(name="my_binding",
                                                                path="hive_format",
                                                                format="csv_with_names",
                                                                connection_id=connection_response.result.connection_id,
                                                                columns=[yearType, monthType, dayType, fruitType, priceType, intervalType],
                                                                projection={
                                                                    "projection.enab": "true",
                                                                    "storage.location.template": "/${year}/${month}/${day}/"
                                                                },
                                                                partitioned_by=["year", "month", "day"],
                                                                check_issues=False)
        assert "Unknown key projection.enab" in str(binding_response.issues)

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    @pytest.mark.parametrize("runtime_listing", [False, True])
    def test_no_schema_columns_except_partitioning_ones(self, kikimr, s3, client, runtime_listing, yq_version):
        if yq_version == "v1" and runtime_listing:
            pytest.skip("Runtime listing is v2 only")

        resource = boto3.resource(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("json_bucket")
        bucket.create(ACL='public-read')

        s3_client = boto3.client(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        json_body = "{}"
        s3_client.put_object(Body=json_body, Bucket='json_bucket', Key='2022/03/obj.json', ContentType='text/json')

        kikimr.control_plane.wait_bootstrap(1)
        client.create_storage_connection("json_bucket", "json_bucket")

        sql = f'''
            pragma s3.UseRuntimeListing="{str(runtime_listing).lower()}";
            ''' + R'''
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

            SELECT
                *
            FROM
                json_bucket.`/`
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

        query_id = client.create_query("simple", sql).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.FAILED)

        describe_result = client.describe_query(query_id).result
        logging.info("AST: {}".format(describe_result.query.ast.data))
        describe_string = "{}".format(describe_result)
        assert "Table contains no columns except partitioning columns" in describe_string, describe_string

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    @pytest.mark.parametrize("runtime_listing", [False, True])
    def test_projection_date(self, kikimr, s3, client, runtime_listing, yq_version):
        if yq_version == "v1" and runtime_listing:
            pytest.skip("Runtime listing is v2 only")

        resource = boto3.resource(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("test_projection_date")
        bucket.create(ACL='public-read')

        s3_client = boto3.client(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        fruits = R'''Fruit,Price,Duration
Banana,3,100'''
        s3_client.put_object(Body=fruits, Bucket='test_projection_date', Key='2022-03-05/fruits.csv', ContentType='text/plain')
        kikimr.control_plane.wait_bootstrap(1)
        connection_response = client.create_storage_connection("fruitbucket", "test_projection_date")

        dt = ydb.Column(name="dt", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.DATE))
        fruitType = ydb.Column(name="Fruit", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING))
        priceType = ydb.Column(name="Price", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT32))
        intervalType = ydb.Column(name="Duration", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT64))
        client.create_object_storage_binding(name="my_binding",
                                             path="/",
                                             format="csv_with_names",
                                             connection_id=connection_response.result.connection_id,
                                             columns=[dt, fruitType, priceType, intervalType],
                                             projection={
                                                 "projection.enabled": "true",
                                                 "projection.dt.type" : "date",
                                                 "projection.dt.min" : "2022-03-05",
                                                 "projection.dt.max" : "2022-03-05",
                                                 "projection.dt.format" : "%F",
                                                 "projection.dt.unit" : "YEARS",
                                                 "storage.location.template": "${dt}"
                                             },
                                             partitioned_by=["dt"])

        sql = f'''
            pragma s3.UseRuntimeListing="{str(runtime_listing).lower()}";

            SELECT *
            FROM bindings.my_binding;
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
    def test_projection_validate_columns(self, kikimr, s3, client):
        resource = boto3.resource(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("test_projection_validate_columns")
        bucket.create(ACL='public-read')

        s3_client = boto3.client(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        fruits = R'''Fruit,Price,Duration
Banana,3,100'''
        s3_client.put_object(Body=fruits, Bucket='test_projection_validate_columns', Key='2022-03-05/fruits.csv', ContentType='text/plain')
        kikimr.control_plane.wait_bootstrap(1)
        connection_response = client.create_storage_connection("fruitbucket", "test_projection_validate_columns")

        dt = ydb.Column(name="dt", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.DATE))
        fruitType = ydb.Column(name="Fruit", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING))
        priceType = ydb.Column(name="Price", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT32))
        intervalType = ydb.Column(name="Duration", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT64))
        binding_response = client.create_object_storage_binding(name="my_binding",
                                                                path="/",
                                                                format="csv_with_names",
                                                                connection_id=connection_response.result.connection_id,
                                                                columns=[dt, fruitType, priceType, intervalType],
                                                                projection={
                                                                    "projection.enabled": "true",
                                                                    "storage.location.template": "${dt}"
                                                                },
                                                                partitioned_by=["dt"], check_issues=False)
        assert "Projection column named dt does not exist for template ${dt}/" in str(binding_response.issues)

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    @pytest.mark.parametrize("runtime_listing", [False, True])
    def test_no_paritioning_columns(self, kikimr, s3, client, runtime_listing, yq_version):
        if yq_version == "v1" and runtime_listing:
            pytest.skip("Runtime listing is v2 only")

        resource = boto3.resource(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("logs2")
        bucket.create(ACL='public-read')

        s3_client = boto3.client(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
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
        s3_client.put_object(Body=json_body, Bucket='logs2', Key='logs/year=2023/month=01/day=16/obj1.json', ContentType='text/json')
        s3_client.put_object(Body=json_body, Bucket='logs2', Key='logs/year=2023/month=01/day=16/obj2.json', ContentType='text/json')

        kikimr.control_plane.wait_bootstrap(1)
        client.create_storage_connection("logs2", "logs2")

        sql = f'''
            pragma s3.UseRuntimeListing="{str(runtime_listing).lower()}";
            ''' + R'''
            $projection = @@ {
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

            SELECT
                count(message)
            FROM
                `logs2`.`/logs`
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
    @pytest.mark.parametrize("client, column_type, is_correct", [
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
        ({"folder_id": "my_folder14"}, "year Date", False)
    ], indirect=["client"])
    @pytest.mark.parametrize("runtime_listing", [False, True])
    def test_projection_integer_type_validation(self, kikimr, s3, client, column_type, is_correct, runtime_listing, yq_version):
        if yq_version == "v1" and runtime_listing:
            pytest.skip("Runtime listing is v2 only")

        resource = boto3.resource(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("test_projection_integer_type_validation")
        bucket.create(ACL='public-read')

        s3_client = boto3.client(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        fruits = R'''Fruit,Price,Duration
Banana,3,100'''
        s3_client.put_object(Body=fruits, Bucket='test_projection_integer_type_validation', Key='2022-03-05/fruits.csv', ContentType='text/plain')
        kikimr.control_plane.wait_bootstrap(1)
        client.create_storage_connection("fruitbucket", "test_projection_integer_type_validation")

        sql = f'''
            pragma s3.UseRuntimeListing="{str(runtime_listing).lower()}";
            ''' + R'''
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
            ''' + R'''
            SELECT *
            FROM `fruitbucket`.`/` WITH
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
            '''.format(column_type=column_type)

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
            assert "Projection column \\\"year\\\" has invalid type" in str(describe_result.query.issue), str(describe_result.query.issue)

    @yq_all
    @pytest.mark.parametrize("client, column_type, is_correct", [
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
    ], indirect=["client"])
    @pytest.mark.parametrize("runtime_listing", [False, True])
    def test_projection_enum_type_invalid_validation(self, kikimr, s3, client, column_type, is_correct, runtime_listing, yq_version):
        if yq_version == "v1" and runtime_listing:
            pytest.skip("Runtime listing is v2 only")

        resource = boto3.resource(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("test_projection_enum_type_invalid_validation")
        bucket.create(ACL='public-read')

        s3_client = boto3.client(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        fruits = R'''Fruit,Price,Duration
Banana,3,100'''
        s3_client.put_object(Body=fruits, Bucket='test_projection_enum_type_invalid_validation', Key='2022-03-05/fruits.csv', ContentType='text/plain')
        kikimr.control_plane.wait_bootstrap(1)
        client.create_storage_connection("fruitbucket", "test_projection_enum_type_invalid_validation")

        sql = f'''
            pragma s3.UseRuntimeListing="{str(runtime_listing).lower()}";
            ''' + R'''
            $projection =
            @@
            {
                "projection.enabled" : true,

                "projection.year.type" : "enum",
                "projection.year.values" : "2022",

                "storage.location.template" : "${year}-03-05"
            }
            @@;
            ''' + R'''
            SELECT *
            FROM `fruitbucket`.`/` WITH
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
            '''.format(column_type=column_type)

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
            assert "Projection column \\\"year\\\" has invalid type" in str(describe_result.query.issue), str(describe_result.query.issue)

    @yq_all
    @pytest.mark.parametrize("client, column_type, is_correct", [
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
    ], indirect=["client"])
    @pytest.mark.parametrize("runtime_listing", [False, True])
    def test_projection_date_type_validation(self, kikimr, s3, client, column_type, is_correct, runtime_listing, yq_version):
        if yq_version == "v1" and runtime_listing:
            pytest.skip("Runtime listing is v2 only")

        resource = boto3.resource(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("test_projection_date_type_invalid_validation")
        bucket.create(ACL='public-read')

        s3_client = boto3.client(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        fruits = R'''Fruit,Price,Duration
Banana,3,100'''
        s3_client.put_object(Body=fruits, Bucket='test_projection_date_type_invalid_validation', Key='2022-03-05/fruits.csv', ContentType='text/plain')
        kikimr.control_plane.wait_bootstrap(1)
        client.create_storage_connection("fruitbucket", "test_projection_date_type_invalid_validation")

        sql = f'''
            pragma s3.UseRuntimeListing="{str(runtime_listing).lower()}";
            ''' + R'''
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
            ''' + R'''
            SELECT *
            FROM `fruitbucket`.`/` WITH
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
            '''.format(column_type=column_type)

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
            assert "Projection column \\\"year\\\" has invalid type" in str(describe_result.query.issue), str(describe_result.query.issue)

    @yq_all
    @pytest.mark.parametrize("client, column_type, is_correct", [
        ({"folder_id": "my_folder1"}, ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT32), True),
        ({"folder_id": "my_folder2"}, ydb.Type(type_id=ydb.Type.PrimitiveTypeId.UINT32), True),
        ({"folder_id": "my_folder3"}, ydb.Type(type_id=ydb.Type.PrimitiveTypeId.UINT64), True),
        ({"folder_id": "my_folder4"}, ydb.Type(type_id=ydb.Type.PrimitiveTypeId.DATE), False),
        ({"folder_id": "my_folder5"}, ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT64), True),
        ({"folder_id": "my_folder6"}, ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING), True),
        ({"folder_id": "my_folder7"}, ydb.Type(type_id=ydb.Type.PrimitiveTypeId.UTF8), True),
        ({"folder_id": "my_folder8"}, ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT32))), False),
        ({"folder_id": "my_folder9"}, ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.UINT32))), False),
        ({"folder_id": "my_folder10"}, ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.UINT64))), False),
        ({"folder_id": "my_folder11"}, ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.DATE))), False),
        ({"folder_id": "my_folder12"}, ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT64))), False),
        ({"folder_id": "my_folder13"}, ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING))), False),
        ({"folder_id": "my_folder14"}, ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.UTF8))), False),
    ], indirect=["client"])
    def test_binding_projection_integer_type_validation(self, kikimr, s3, client, column_type, is_correct):
        resource = boto3.resource(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("test_binding_projection_integer_type_validation")
        bucket.create(ACL='public-read')

        s3_client = boto3.client(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        fruits = R'''Fruit,Price,Duration
Banana,3,100'''
        s3_client.put_object(Body=fruits, Bucket='test_binding_projection_integer_type_validation', Key='2022-03-05/fruits.csv', ContentType='text/plain')
        kikimr.control_plane.wait_bootstrap(1)
        connection_response = client.create_storage_connection("fruitbucket", "test_binding_projection_integer_type_validation")

        year = ydb.Column(name="year", type=column_type)
        fruitType = ydb.Column(name="Fruit", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING))
        binding_response = client.create_object_storage_binding(name="my_binding",
                                                                path="/",
                                                                format="csv_with_names",
                                                                connection_id=connection_response.result.connection_id,
                                                                columns=[year, fruitType],
                                                                projection={
                                                                    "projection.enabled": "true",
                                                                    "projection.year.type" : "integer",
                                                                    "projection.year.min" : "2022",
                                                                    "projection.year.max" : "2022",
                                                                    "storage.location.template": "${year}-03-05"
                                                                },
                                                                partitioned_by=["year"], check_issues=is_correct)
        if not is_correct:
            assert "Column \\\"year\\\" from projection does not support" in str(binding_response.issues)

    @yq_all
    @pytest.mark.parametrize("client, column_type, is_correct", [
        ({"folder_id": "my_folder1"}, ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT32), False),
        ({"folder_id": "my_folder2"}, ydb.Type(type_id=ydb.Type.PrimitiveTypeId.UINT32), False),
        ({"folder_id": "my_folder3"}, ydb.Type(type_id=ydb.Type.PrimitiveTypeId.UINT64), False),
        ({"folder_id": "my_folder4"}, ydb.Type(type_id=ydb.Type.PrimitiveTypeId.DATE), False),
        ({"folder_id": "my_folder5"}, ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT64), False),
        ({"folder_id": "my_folder6"}, ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING), True),
        ({"folder_id": "my_folder7"}, ydb.Type(type_id=ydb.Type.PrimitiveTypeId.UTF8), False),
        ({"folder_id": "my_folder8"}, ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT32))), False),
        ({"folder_id": "my_folder9"}, ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.UINT32))), False),
        ({"folder_id": "my_folder10"}, ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.UINT64))), False),
        ({"folder_id": "my_folder11"}, ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.DATE))), False),
        ({"folder_id": "my_folder12"}, ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT64))), False),
        ({"folder_id": "my_folder13"}, ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING))), False),
        ({"folder_id": "my_folder14"}, ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.UTF8))), False),
    ], indirect=["client"])
    def test_binding_projection_enum_type_validation(self, kikimr, s3, client, column_type, is_correct):
        resource = boto3.resource(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("test_binding_projection_enum_type_validation")
        bucket.create(ACL='public-read')

        s3_client = boto3.client(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        fruits = R'''Fruit,Price,Duration
Banana,3,100'''
        s3_client.put_object(Body=fruits, Bucket='test_binding_projection_enum_type_validation', Key='2022-03-05/fruits.csv', ContentType='text/plain')
        kikimr.control_plane.wait_bootstrap(1)
        connection_response = client.create_storage_connection("fruitbucket", "test_binding_projection_enum_type_validation")

        year = ydb.Column(name="year", type=column_type)
        fruitType = ydb.Column(name="Fruit", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING))
        binding_response = client.create_object_storage_binding(name="my_binding",
                                                                path="/",
                                                                format="csv_with_names",
                                                                connection_id=connection_response.result.connection_id,
                                                                columns=[year, fruitType],
                                                                projection={
                                                                    "projection.enabled": "true",
                                                                    "projection.year.type" : "enum",
                                                                    "projection.year.values" : "2022",
                                                                    "storage.location.template": "${year}-03-05"
                                                                },
                                                                partitioned_by=["year"], check_issues=is_correct)
        if not is_correct:
            assert "Column \\\"year\\\" from projection does not support" in str(binding_response.issues)

    @yq_all
    @pytest.mark.parametrize("client, column_type, is_correct", [
        ({"folder_id": "my_folder1"}, ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT32), False),
        ({"folder_id": "my_folder2"}, ydb.Type(type_id=ydb.Type.PrimitiveTypeId.UINT32), True),
        ({"folder_id": "my_folder3"}, ydb.Type(type_id=ydb.Type.PrimitiveTypeId.UINT64), False),
        ({"folder_id": "my_folder4"}, ydb.Type(type_id=ydb.Type.PrimitiveTypeId.DATE), True),
        ({"folder_id": "my_folder5"}, ydb.Type(type_id=ydb.Type.PrimitiveTypeId.DATETIME), True),
        ({"folder_id": "my_folder6"}, ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT64), False),
        ({"folder_id": "my_folder7"}, ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING), True),
        ({"folder_id": "my_folder8"}, ydb.Type(type_id=ydb.Type.PrimitiveTypeId.UTF8), True),
        ({"folder_id": "my_folder9"}, ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT32))), False),
        ({"folder_id": "my_folder10"}, ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.UINT32))), False),
        ({"folder_id": "my_folder11"}, ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.UINT64))), False),
        ({"folder_id": "my_folder12"}, ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.DATE))), False),
        ({"folder_id": "my_folder13"}, ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.DATETIME))), False),
        ({"folder_id": "my_folder14"}, ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT64))), False),
        ({"folder_id": "my_folder15"}, ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING))), False),
        ({"folder_id": "my_folder16"}, ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.UTF8))), False),
    ], indirect=["client"])
    def test_binding_projection_date_type_validation(self, kikimr, s3, client, column_type, is_correct):
        resource = boto3.resource(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("test_binding_projection_date_type_validation")
        bucket.create(ACL='public-read')

        s3_client = boto3.client(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        fruits = R'''Fruit,Price,Duration
Banana,3,100'''
        s3_client.put_object(Body=fruits, Bucket='test_binding_projection_date_type_validation', Key='2022-03-05/fruits.csv', ContentType='text/plain')
        kikimr.control_plane.wait_bootstrap(1)
        connection_response = client.create_storage_connection("fruitbucket", "test_binding_projection_date_type_validation")

        year = ydb.Column(name="year", type=column_type)
        fruitType = ydb.Column(name="Fruit", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING))
        binding_response = client.create_object_storage_binding(name="my_binding",
                                                                path="/",
                                                                format="csv_with_names",
                                                                connection_id=connection_response.result.connection_id,
                                                                columns=[year, fruitType],
                                                                projection={
                                                                    "projection.enabled": "true",
                                                                    "projection.year.type" : "date",
                                                                    "projection.year.min" : "2022-01-01",
                                                                    "projection.year.max" : "2022-01-01",
                                                                    "projection.year.format" : "%Y",
                                                                    "projection.year.interval" : "1",
                                                                    "projection.year.unit" : "DAYS",
                                                                    "storage.location.template": "${year}-03-05"
                                                                },
                                                                partitioned_by=["year"], check_issues=is_correct)
        if not is_correct:
            assert "Column \\\"year\\\" from projection does not support" in str(binding_response.issues)

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    @pytest.mark.parametrize("runtime_listing", [False, True])
    def test_raw_format(self, kikimr, s3, client, runtime_listing, yq_version):
        if yq_version == "v1" and runtime_listing:
            pytest.skip("Runtime listing is v2 only")

        resource = boto3.resource(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("raw_bucket")
        bucket.create(ACL='public-read')

        s3_client = boto3.client(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        s3_client.put_object(Body='text',
                             Bucket='raw_bucket',
                             Key='raw_format/year=2023/month=01/day=14/file1.txt',
                             ContentType='text/plain')

        kikimr.control_plane.wait_bootstrap(1)
        client.create_storage_connection("rawbucket", "raw_bucket")

        sql = f'''
            pragma s3.UseRuntimeListing="{str(runtime_listing).lower()}";
            ''' + R'''
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

            SELECT
                data,
                timestamp
            FROM
                `rawbucket`.`raw_format`
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
    @pytest.mark.parametrize("blocks", [False, True])
    @pytest.mark.parametrize("runtime_listing", [False, True])
    def test_parquet(self, kikimr, s3, blocks, client, runtime_listing, yq_version):
        if yq_version == "v1" and runtime_listing:
            pytest.skip("Runtime listing is v2 only")

        resource = boto3.resource(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("parquets")
        bucket.create(ACL='public-read-write')
        bucket.objects.all().delete()

        client.create_storage_connection("pb", "parquets")

        sql = R'''
            insert into pb.`part/x=1/` with (format="parquet")
            select * from AS_TABLE([<|foo:123, bar:"xxx"u|>,<|foo:456, bar:"yyy"u|>]);
            '''
        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        sql = R'''
            insert into pb.`part/x=2/` with (format="parquet")
            select * from AS_TABLE([<|foo:234, bar:"zzz"u|>,<|foo:567, bar:"ttt"u|>]);
            '''
        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        sql = f'''
            pragma s3.UseBlocksSource="{str(blocks).lower()}";
            pragma s3.UseRuntimeListing="{str(runtime_listing).lower()}";

            SELECT foo, bar, x FROM pb.`part/`
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
