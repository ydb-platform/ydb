#!/usr/bin/env python
# -*- coding: utf-8 -*-

import boto3
import logging

import pytest


import ydb.public.api.protos.ydb_value_pb2 as ydb


class TestS3:
    @pytest.mark.parametrize(
        'column_type, is_correct',
        [
            ('year Int32', False),
            ('year Int32 NOT NULL', False),
            ('year Uint32', False),
            ('year Uint32 NOT NULL', True),
            ('year Int64', False),
            ('year Int64 NOT NULL', False),
            ('year Uint64', False),
            ('year Uint64 NOT NULL', False),
            ('year String NOT NULL', True),
            ('year String', False),
            ('year Utf8', False),
            ('year Utf8 NOT NULL', True),
            ('year Date', False),
            ('year Date NOT NULL', True),
            ('year Datetime', False),
            ('year Datetime NOT NULL', True),
        ],
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
        storage_connection_name = client.path(unique_prefix + "fruitbucket")
        client.create_external_data_source(storage_connection_name, "test_projection_date_type_invalid_validation")

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

        query = client.execute(sql)
        if is_correct:
            query.check()
            data = query.result_set()
            result_set = data
            logging.debug(str(result_set))
            assert len(result_set.columns) == 2
            assert result_set.columns[0].name == "Fruit"
            assert result_set.columns[0].type.type_id == ydb.Type.STRING
            assert len(result_set.rows) == 1
            assert result_set.rows[0][0] == b"Banana"
        else:
            query.check(False)
            describe_result = query
            logging.info("AST: {}".format(describe_result.ast))
            assert 'Projection column "year" has invalid type' in str(describe_result.error), str(describe_result.error)

    @pytest.mark.parametrize(
        'column_type, is_correct',
        [
            (ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT32), True),
            (ydb.Type(type_id=ydb.Type.PrimitiveTypeId.UINT32), True),
            (ydb.Type(type_id=ydb.Type.PrimitiveTypeId.UINT64), True),
            (ydb.Type(type_id=ydb.Type.PrimitiveTypeId.DATE), False),
            (ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT64), True),
            (ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING), True),
            (ydb.Type(type_id=ydb.Type.PrimitiveTypeId.UTF8), True),
            (ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT32))), False),
            (ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.UINT32))), False),
            (ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.UINT64))), False),
            (ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.DATE))), False),
            (ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT64))), False),
            (ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING))), False),
            (ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.UTF8))), False),
        ],
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
        connection_response = client.create_external_data_source(
            unique_prefix + "fruitbucket", "test_binding_projection_integer_type_validation"
        )

        year = ydb.Column(name="year", type=column_type)
        fruitType = ydb.Column(name="Fruit", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING))
        binding_response = client.create_external_table(
            name=unique_prefix + "my_binding",
            path="/",
            format="csv_with_names",
            data_source=connection_response.name,
            columns=[year, fruitType],
            projection={
                "projection.enabled": "true",
                "projection.year.type": "integer",
                "projection.year.min": "2022",
                "projection.year.max": "2022",
                "storage.location.template": "${year}-03-05",
            },
            partitioned_by=["year"],
            expect_error=not is_correct,
        )
        if not is_correct:
            assert 'Column "year" from projection does not support' in str(binding_response.error)

    @pytest.mark.parametrize(
        'column_type, is_correct',
        [
            (ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT32), False),
            (ydb.Type(type_id=ydb.Type.PrimitiveTypeId.UINT32), False),
            (ydb.Type(type_id=ydb.Type.PrimitiveTypeId.UINT64), False),
            (ydb.Type(type_id=ydb.Type.PrimitiveTypeId.DATE), False),
            (ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT64), False),
            (ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING), True),
            (ydb.Type(type_id=ydb.Type.PrimitiveTypeId.UTF8), False),
            (ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT32))), False),
            (ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.UINT32))), False),
            (ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.UINT64))), False),
            (ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.DATE))), False),
            (ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT64))), False),
            (ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING))), False),
            (ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.UTF8))), False),
        ],
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
        connection_response = client.create_external_data_source(
            unique_prefix + "fruitbucket", "test_binding_projection_enum_type_validation"
        )

        year = ydb.Column(name="year", type=column_type)
        fruitType = ydb.Column(name="Fruit", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING))
        binding_response = client.create_external_table(
            name=unique_prefix + "my_binding",
            path="/",
            format="csv_with_names",
            data_source=connection_response.name,
            columns=[year, fruitType],
            projection={
                "projection.enabled": "true",
                "projection.year.type": "enum",
                "projection.year.values": "2022",
                "storage.location.template": "${year}-03-05",
            },
            partitioned_by=["year"],
            expect_error=not is_correct,
        )
        if not is_correct:
            assert 'Column "year" from projection does not support' in str(binding_response.error)

    @pytest.mark.parametrize(
        'column_type, is_correct',
        [
            (ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT32), False),
            (ydb.Type(type_id=ydb.Type.PrimitiveTypeId.UINT32), True),
            (ydb.Type(type_id=ydb.Type.PrimitiveTypeId.UINT64), False),
            (ydb.Type(type_id=ydb.Type.PrimitiveTypeId.DATE), True),
            (ydb.Type(type_id=ydb.Type.PrimitiveTypeId.DATETIME), True),
            (ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT64), False),
            (ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING), True),
            (ydb.Type(type_id=ydb.Type.PrimitiveTypeId.UTF8), True),
            (ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT32))), False),
            (ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.UINT32))), False),
            (ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.UINT64))), False),
            (ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.DATE))), False),
            (ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.DATETIME))), False),
            (ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT64))), False),
            (ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING))), False),
            (ydb.Type(optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.UTF8))), False),
        ],
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
        connection_response = client.create_external_data_source(
            unique_prefix + "fruitbucket", "test_binding_projection_date_type_validation"
        )

        year = ydb.Column(name="year", type=column_type)
        fruitType = ydb.Column(name="Fruit", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING))
        binding_response = client.create_external_table(
            name=unique_prefix + "my_binding",
            path="/",
            format="csv_with_names",
            data_source=connection_response.name,
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
            expect_error=not is_correct,
        )
        if not is_correct:
            assert 'Column "year" from projection does not support' in str(binding_response.error)

    @pytest.mark.parametrize("runtime_listing", ["false", "true"])
    def test_raw_format(self, kikimr, s3, client, runtime_listing, unique_prefix):
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

        storage_connection_name = client.path(unique_prefix + "rawbucket")
        client.create_external_data_source(storage_connection_name, "raw_bucket")

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

        query = client.execute(sql)
        query.check()

        describe_result = query
        logging.info("AST: {}".format(describe_result.ast))

        data = query.result_set()
        result_set = data
        logging.debug(str(result_set))

        assert len(result_set.columns) == 2
        assert result_set.columns[0].name == "data"
        assert result_set.columns[0].type.type_id == ydb.Type.STRING
        assert result_set.columns[1].name == "timestamp"
        assert result_set.columns[1].type.type_id == ydb.Type.DATE
        assert len(result_set.rows) == 1
        assert result_set.rows[0][0] == b"text"
        assert result_set.rows[0][1] == 19371

    @pytest.mark.parametrize("runtime_listing", ["false", "true"])
    def test_parquet(self, kikimr, s3, client, runtime_listing, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("parquets")
        bucket.create(ACL='public-read-write')
        bucket.objects.all().delete()

        storage_connection_name = client.path(unique_prefix + "pb")
        client.create_external_data_source(storage_connection_name, "parquets")

        sql = fR'''
            insert into `{storage_connection_name}`.`part/x=1/` with (format="parquet")
            select * from AS_TABLE([<|foo:123, bar:"xxx"u|>,<|foo:456, bar:"yyy"u|>]);
            '''
        query = client.execute(sql)
        query.check()

        sql = fR'''
            insert into `{storage_connection_name}`.`part/x=2/` with (format="parquet")
            select * from AS_TABLE([<|foo:234, bar:"zzz"u|>,<|foo:567, bar:"ttt"u|>]);
            '''
        query = client.execute(sql)
        query.check()

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

        query = client.execute(sql)
        query.check()

        data = query.result_set()
        result_set = data
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
        assert result_set.rows[0][0] == 123
        assert result_set.rows[1][0] == 234
        assert result_set.rows[2][0] == 456
        assert result_set.rows[3][0] == 567
        assert result_set.rows[0][1] == b"xxx"
        assert result_set.rows[1][1] == b"zzz"
        assert result_set.rows[2][1] == b"yyy"
        assert result_set.rows[3][1] == b"ttt"
        assert result_set.rows[0][2] == 1
        assert result_set.rows[1][2] == 2
        assert result_set.rows[2][2] == 1
        assert result_set.rows[3][2] == 2

    @pytest.mark.parametrize("runtime_listing", ["true"])
    def test_valid_projected_column_values(self, kikimr, s3, client, runtime_listing, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("bucket")
        bucket.create(ACL='public-read')

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        s3_client.put_object(
            Body='''data
text1''',
            Bucket='bucket',
            Key='2024/01/file.txt',
            ContentType='text/plain',
        )
        s3_client.put_object(
            Body='''data
text2''',
            Bucket='bucket',
            Key='2024/02/file.txt',
            ContentType='text/plain',
        )
        s3_client.put_object(
            Body='''data
text3''',
            Bucket='bucket',
            Key='2025/01/file.txt',
            ContentType='text/plain',
        )

        storage_connection_name = client.path(unique_prefix + "bucket")
        client.create_external_data_source(storage_connection_name, "bucket")

        sql = (
            f'''
            pragma s3.UseRuntimeListing="{runtime_listing}";
            '''
            + R'''
            $projection = @@ {
                "projection.enabled" : true,

                "projection.year.type" : "integer",
                "projection.year.min" : 2024,
                "projection.year.max" : 2025,
                "projection.year.interval" : 1,

                "projection.month.type" : "integer",
                "projection.month.min" : 1,
                "projection.month.max" : 2,
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
                format=csv_with_names,
                schema=(
                    data String NOT NULL,
                    year Int NOT NULL,
                    month Int NOT NULL,
                ),
                partitioned_by=(year, month),
                projection=$projection
            )
            WHERE data ILIKE '%text2%';
        '''
        )

        # temporary fix for dynamic listing

        query = client.execute(sql)
        query.check()

        describe_result = query
        logging.info("AST: {}".format(describe_result.ast))

        data = query.result_set()
        result_set = data
        logging.debug(str(result_set))

        assert len(result_set.columns) == 3
        assert result_set.columns[0].name == "data"
        assert result_set.columns[0].type.type_id == ydb.Type.STRING
        assert result_set.columns[1].name == "month"
        assert result_set.columns[1].type.type_id == ydb.Type.INT32
        assert result_set.columns[2].name == "year"
        assert result_set.columns[2].type.type_id == ydb.Type.INT32
        assert len(result_set.rows) == 1
        assert result_set.rows[0][0] == b"text2"
        assert result_set.rows[0][1] == 2
        assert result_set.rows[0][2] == 2024
