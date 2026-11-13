#!/usr/bin/env python
# -*- coding: utf-8 -*-

from decimal import Decimal

import boto3
import logging

import ydb.public.api.protos.ydb_value_pb2 as ydb

from ydb.tests.fq.s3.s3_helpers import create_bucket_and_put_object
from ydb.tests.library.common.wait_for import wait_for


class TestExternalTables:
    def test_s3_insert(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("bindbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        data_source = client.create_external_data_source(unique_prefix + "bb", "bindbucket").name

        fooType = ydb.Column(name="foo", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT32))
        barType = ydb.Column(name="bar", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.UTF8))
        storage_binding_name = unique_prefix + "s3binding"
        client.create_external_table(
            name=storage_binding_name,
            path="path1/",
            format="csv_with_names",
            data_source=data_source,
            columns=[fooType, barType],
        )

        sql = fR'''
            insert into `{storage_binding_name}`
            select * from AS_TABLE([<|foo:123, bar:"xxx"u|>,<|foo:456, bar:"yyy"u|>]);
            '''

        query = client.execute(sql)
        query.check()

        sql = fR'''
            select foo, bar from `{storage_binding_name}`;
            '''

        query = client.execute(sql)
        query.check()

        data = query.result_set()
        result_set = data
        assert len(result_set.columns) == 2
        assert result_set.columns[0].name == "foo"
        assert result_set.columns[0].type.type_id == ydb.Type.INT32
        assert result_set.columns[1].name == "bar"
        assert result_set.columns[1].type.type_id == ydb.Type.UTF8
        assert len(result_set.rows) == 2
        assert result_set.rows[0][0] == 123
        assert result_set.rows[0][1] == 'xxx'
        assert result_set.rows[1][0] == 456
        assert result_set.rows[1][1] == 'yyy'

    def test_s3_format_mismatch(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("bindbucket")
        bucket.create(ACL='public-read')

        data_source = client.create_external_data_source(unique_prefix + "bb", "bindbucket").name

        fooType = ydb.Column(name="foo", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.UTF8))
        barType = ydb.Column(name="bar", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT32))
        storage_binding_name = unique_prefix + "s3binding"
        client.create_external_table(
            name=storage_binding_name,
            path="path2/",
            format="csv_with_names",
            data_source=data_source,
            columns=[fooType, barType],
        )

        sql = fR'''
            insert into `{storage_binding_name}`
            select * from AS_TABLE([<|foo:123, bar:"xxx"u|>,<|foo:456, bar:"yyy"u|>]);
            '''

        query = client.execute(sql)
        query.check(False)

        describe_result = query
        describe_string = "{}".format(describe_result)
        assert "Row type mismatch for S3 external table" in describe_string, describe_string

    def test_count_for_pg_binding(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("count_for_pg_binding")
        bucket.create(ACL='public-read')

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        row = R'''{"a": 42, "b": 3.14, "c": "text"}'''
        s3_client.put_object(Body=row, Bucket='count_for_pg_binding', Key='abc.json', ContentType='text/json')

        connection_response = client.create_external_data_source(unique_prefix + "abc", "count_for_pg_binding")

        aType = ydb.Column(name="a", type=ydb.Type(pg_type=ydb.PgType(oid=23)))
        bType = ydb.Column(name="b", type=ydb.Type(pg_type=ydb.PgType(oid=701)))
        cType = ydb.Column(name="c", type=ydb.Type(pg_type=ydb.PgType(oid=25)))
        storage_binding_name = unique_prefix + "binding_for_count"
        client.create_external_table(
            name=storage_binding_name,
            path="abc.json",
            format="json_each_row",
            data_source=connection_response.name,
            columns=[aType, bType, cType],
        )

        sql = fR'''
            SELECT COUNT(*)
            FROM {storage_binding_name};
            '''

        query = client.execute(sql)
        query.check()

        data = query.result_set()
        result_set = data
        logging.debug(str(result_set))
        assert len(result_set.columns) == 1
        assert len(result_set.rows) == 1
        assert result_set.columns[0].type.type_id == ydb.Type.UINT64
        assert result_set.rows[0][0] == 1

    def test_unknown_column_in_external_table(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("bindbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        data_source = client.create_external_data_source(unique_prefix + "bb", "bindbucket").name

        data_column = ydb.Column(name="data", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING))
        storage_binding_name = unique_prefix + "s3binding"
        client.create_external_table(
            name=storage_binding_name, path="/", format="raw", data_source=data_source, columns=[data_column]
        )

        sql = fR'''
            SELECT some_unknown_column FROM `{storage_binding_name}`;
        '''

        query = client.execute(sql)
        query.check(False)

        assert "some_unknown_column" in query.error

    def test_ast_in_failed_query_compilation(self, kikimr, s3, client, unique_prefix):
        create_bucket_and_put_object(s3.s3_url, 'failed-compilation-ast', 'data.txt', 'dictionary')
        source = client.create_external_data_source(unique_prefix + 'source', 'failed-compilation-ast').name
        table = client.path(unique_prefix + 'dictionary')

        def create_table(column):
            client.create_external_table(
                table,
                'data.txt',
                'raw',
                source,
                [ydb.Column(name=column, type=ydb.Type(type_id=ydb.Type.STRING))],
            )

        create_table('some_unknown_column')
        input_topic = client.path(unique_prefix + 'input')
        output_topic = client.path(unique_prefix + 'output')
        for topic in (input_topic, output_topic):
            client.query(f'CREATE TOPIC `{topic}`;')
            client.objects.append(('TOPIC', topic))
        query_path = client.path(unique_prefix + 'failed_compilation')
        state = None

        def failed_with_ast():
            nonlocal state
            rows = kikimr.ydb_client.query(f"""
                SELECT Status, Issues, Ast FROM `.sys/streaming_queries`
                WHERE Path = '{query_path}';
            """)[0].rows
            state = rows[0] if rows else None
            return state is not None and state['Status'] == 'FAILED' and bool(state['Ast'])

        try:
            # Validate the definition before changing the schema. Starting it
            # must then fail compilation and retain the AST in the sys view.
            client.query(f"""
                CREATE STREAMING QUERY `{query_path}` WITH (RUN = FALSE) AS DO BEGIN
                    INSERT INTO `{output_topic}`
                    SELECT input_rows.Data || dictionary.some_unknown_column AS Data
                    FROM `{input_topic}`
                    WITH (FORMAT=raw, SCHEMA=(Data String NOT NULL)) AS input_rows
                    CROSS JOIN `{table}` AS dictionary;
                END DO;
            """)
            client.drop('EXTERNAL TABLE', table)
            create_table('data')
            result = client.execute(f'ALTER STREAMING QUERY `{query_path}` SET (RUN = TRUE);').check(False)
            assert 'some_unknown_column' in result.error, result.error
            assert wait_for(failed_with_ast, timeout_seconds=120, step_seconds=0.5), state
            assert 'some_unknown_column' in state['Issues'], state
            assert 'some_unknown_column' in state['Ast'], state['Ast']
            assert table in state['Ast'], state['Ast']
        finally:
            client.query(f'DROP STREAMING QUERY IF EXISTS `{query_path}`;')

    def test_external_table_empty_schema(self, kikimr, client, unique_prefix):
        connection_response = client.create_external_data_source(unique_prefix + "fruitbucket", "fbucket")
        binding_response = client.create_external_table(
            name=unique_prefix + "my_binding",
            path="fruits.csv",
            format="raw",
            data_source=connection_response.name,
            columns=[],
            expect_error=True,
        )
        assert binding_response.error

    def test_external_table_with_backslash_in_location(self, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("backslash_bucket")
        bucket.create(ACL='public-read')

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        data = R'''data
test'''
        s3_client.put_object(Body=data, Bucket='backslash_bucket', Key='\\', ContentType='text/plain')

        connection_response = client.create_external_data_source(unique_prefix + "backslash_bucket", "backslash_bucket")

        data_type = ydb.Column(name="data", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.UTF8))
        storage_binding_name = unique_prefix + "binding_name"
        client.create_external_table(
            name=storage_binding_name,
            path="\\",
            format="csv_with_names",
            data_source=connection_response.name,
            columns=[data_type],
        )

        sql = fR'''
            SELECT *
            FROM {storage_binding_name};
            '''

        query = client.execute(sql)
        query.check()

        data = query.result_set()
        result_set = data
        logging.debug(str(result_set))
        assert len(result_set.columns) == 1
        assert result_set.columns[0].name == "data"
        assert len(result_set.rows) == 1
        assert result_set.rows[0][0] == "test"

    def test_decimal_external_table(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("decimal_binding")
        bucket.create(ACL='public-read')

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        row = R'''
            { "a": 0.1111111115, "b": 0.111111115 },
            { "a": 0.1111111115, "b": 0.111111115 },
        '''
        s3_client.put_object(Body=row, Bucket='decimal_binding', Key='abc.json', ContentType='text/json')

        connection_response = client.create_external_data_source(unique_prefix + "_decc", "decimal_binding")

        aType = ydb.Column(name="a", type=ydb.Type(decimal_type=ydb.DecimalType(precision=22, scale=9)))
        bType = ydb.Column(name="b", type=ydb.Type(decimal_type=ydb.DecimalType(precision=22, scale=9)))

        storage_binding_name = unique_prefix + "_decb"
        client.create_external_table(
            name=storage_binding_name,
            path="abc.json",
            format="json_each_row",
            data_source=connection_response.name,
            columns=[aType, bType],
        )

        sql = fR'''
            SELECT Unwrap(SUM(a)) as a, Unwrap(SUM(b)) as b
            FROM {storage_binding_name};
            '''

        query = client.execute(sql)
        query.check()

        data = query.result_set()
        result_set = data
        logging.debug(str(result_set))
        assert len(result_set.columns) == 2
        assert len(result_set.rows) == 1
        assert result_set.columns[0].type == ydb.Type(decimal_type=ydb.DecimalType(precision=35, scale=9))
        assert result_set.columns[1].type == ydb.Type(decimal_type=ydb.DecimalType(precision=35, scale=9))
        assert result_set.rows[0][0] == Decimal("0.222222222")
        assert result_set.rows[0][1] == Decimal("0.222222230")
