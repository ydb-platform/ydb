#!/usr/bin/env python
# -*- coding: utf-8 -*-

import boto3
import logging
import pytest

import ydb.public.api.protos.ydb_value_pb2 as ydb
import ydb.public.api.protos.draft.fq_pb2 as fq
from ydb.tests.tools.fq_runner.kikimr_utils import yq_all


class TestBindings:
    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    @pytest.mark.parametrize("kikimr_settings", [{"bindings_mode": "BM_DROP_WITH_WARNING"}], indirect=True)
    def test_s3_insert(self, kikimr, s3, client, yq_version, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("bindbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        kikimr.control_plane.wait_bootstrap(1)
        connection_id = client.create_storage_connection(unique_prefix + "bb", "bindbucket").result.connection_id

        fooType = ydb.Column(name="foo", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT32))
        barType = ydb.Column(name="bar", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.UTF8))
        storage_binding_name = unique_prefix + "s3binding"
        client.create_object_storage_binding(
            name=storage_binding_name,
            path="path1/",
            format="csv_with_names",
            connection_id=connection_id,
            columns=[fooType, barType],
        )

        sql = fR'''
            insert into bindings.`{storage_binding_name}`
            select * from AS_TABLE([<|foo:123, bar:"xxx"u|>,<|foo:456, bar:"yyy"u|>]);
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
        if yq_version == "v2":
            issues = str(client.describe_query(query_id).result.query.issue)
            assert (
                "message: \"Please remove \\\'bindings.\\\' from your query, the support for this syntax will be dropped soon"
                in issues
            )
            assert "severity: 2" in issues

        sql = fR'''
            select foo, bar from bindings.`{storage_binding_name}`;
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
        if yq_version == "v2":
            issues = str(client.describe_query(query_id).result.query.issue)
            assert (
                "message: \"Please remove \\\'bindings.\\\' from your query, the support for this syntax will be dropped soon"
                in issues
            )
            assert "severity: 2" in issues

        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        assert len(result_set.columns) == 2
        assert result_set.columns[0].name == "foo"
        assert result_set.columns[0].type.type_id == ydb.Type.INT32
        assert result_set.columns[1].name == "bar"
        assert result_set.columns[1].type.type_id == ydb.Type.UTF8
        assert len(result_set.rows) == 2
        assert result_set.rows[0].items[0].int32_value == 123
        assert result_set.rows[0].items[1].text_value == 'xxx'
        assert result_set.rows[1].items[0].int32_value == 456
        assert result_set.rows[1].items[1].text_value == 'yyy'

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_s3_format_mismatch(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("bindbucket")
        bucket.create(ACL='public-read')

        kikimr.control_plane.wait_bootstrap(1)
        connection_id = client.create_storage_connection(unique_prefix + "bb", "bindbucket").result.connection_id

        fooType = ydb.Column(name="foo", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.UTF8))
        barType = ydb.Column(name="bar", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT32))
        storage_binding_name = unique_prefix + "s3binding"
        client.create_object_storage_binding(
            name=storage_binding_name,
            path="path2/",
            format="csv_with_names",
            connection_id=connection_id,
            columns=[fooType, barType],
        )

        sql = fR'''
            insert into bindings.`{storage_binding_name}`
            select * from AS_TABLE([<|foo:123, bar:"xxx"u|>,<|foo:456, bar:"yyy"u|>]);
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query(query_id, statuses=[fq.QueryMeta.FAILED])

        describe_result = client.describe_query(query_id).result
        describe_string = "{}".format(describe_result)
        assert "Type mismatch between schema type" in describe_string, describe_string

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_pg_binding(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("fbucket")
        bucket.create(ACL='public-read')

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        fruits = R'''Fruit,Price
Banana,3
Apple,2
Pear,15'''
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='a/fruits.csv', ContentType='text/plain')

        kikimr.control_plane.wait_bootstrap(1)
        connection_response = client.create_storage_connection(unique_prefix + "fruitbucket", "fbucket")

        fruitType = ydb.Column(name="Fruit", type=ydb.Type(pg_type=ydb.PgType(oid=25)))
        priceType = ydb.Column(name="Price", type=ydb.Type(pg_type=ydb.PgType(oid=23)))
        storage_binding_name = unique_prefix + "my_binding"
        client.create_object_storage_binding(
            name=storage_binding_name,
            path="a/",
            format="csv_with_names",
            connection_id=connection_response.result.connection_id,
            columns=[fruitType, priceType],
            format_setting={"file_pattern": "*.csv"},
        )

        sql = fR'''
            SELECT *
            FROM bindings.{storage_binding_name};
            '''

        query_id = client.create_query(
            "simple", sql, type=fq.QueryContent.QueryType.ANALYTICS, pg_syntax=True
        ).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        logging.debug(str(result_set))
        assert len(result_set.columns) == 2
        assert result_set.columns[0].name == "Fruit"
        assert result_set.columns[0].type.pg_type.oid == 25
        assert result_set.columns[1].name == "Price"
        assert result_set.columns[1].type.pg_type.oid == 23
        assert len(result_set.rows) == 3
        assert result_set.rows[0].items[0].text_value == "Banana"
        assert result_set.rows[0].items[1].text_value == "3"
        assert result_set.rows[1].items[0].text_value == "Apple"
        assert result_set.rows[1].items[1].text_value == "2"
        assert result_set.rows[2].items[0].text_value == "Pear"
        assert result_set.rows[2].items[1].text_value == "15"

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    @pytest.mark.parametrize("pg_syntax", [False, True], ids=["yql_syntax", "pg_syntax"])
    def test_count_for_pg_binding(self, kikimr, s3, client, pg_syntax, unique_prefix):
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

        kikimr.control_plane.wait_bootstrap(1)
        connection_response = client.create_storage_connection(unique_prefix + "abc", "count_for_pg_binding")

        aType = ydb.Column(name="a", type=ydb.Type(pg_type=ydb.PgType(oid=23)))
        bType = ydb.Column(name="b", type=ydb.Type(pg_type=ydb.PgType(oid=701)))
        cType = ydb.Column(name="c", type=ydb.Type(pg_type=ydb.PgType(oid=25)))
        storage_binding_name = unique_prefix + "binding_for_count"
        client.create_object_storage_binding(
            name=storage_binding_name,
            path="abc.json",
            format="json_each_row",
            connection_id=connection_response.result.connection_id,
            columns=[aType, bType, cType],
        )

        sql = fR'''
            SELECT COUNT(*)
            FROM bindings.{storage_binding_name};
            '''

        query_id = client.create_query(
            "simple", sql, type=fq.QueryContent.QueryType.ANALYTICS, pg_syntax=pg_syntax
        ).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        logging.debug(str(result_set))
        assert len(result_set.columns) == 1
        assert len(result_set.rows) == 1
        if pg_syntax:
            assert result_set.columns[0].type.pg_type.oid == 20
            assert result_set.rows[0].items[0].text_value == "1"
        else:
            assert result_set.columns[0].type.type_id == ydb.Type.UINT64
            assert result_set.rows[0].items[0].uint64_value == 1

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_ast_in_failed_query_compilation(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("bindbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        kikimr.control_plane.wait_bootstrap(1)
        connection_id = client.create_storage_connection(unique_prefix + "bb", "bindbucket").result.connection_id

        data_column = ydb.Column(name="data", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING))
        storage_binding_name = unique_prefix + "s3binding"
        client.create_object_storage_binding(
            name=storage_binding_name, path="/", format="raw", connection_id=connection_id, columns=[data_column]
        )

        sql = fR'''
            SELECT some_unknown_column FROM bindings.`{storage_binding_name}`;
        '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.FAILED)

        ast = client.describe_query(query_id).result.query.ast.data
        assert "(\'columns \'(\'\"some_unknown_column\"))" in ast, "Invalid query ast"

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_raw_empty_schema_binding(self, kikimr, client, unique_prefix):
        kikimr.control_plane.wait_bootstrap(1)
        connection_response = client.create_storage_connection(unique_prefix + "fruitbucket", "fbucket")
        binding_response = client.create_object_storage_binding(
            name=unique_prefix + "my_binding",
            path="fruits.csv",
            format="raw",
            connection_id=connection_response.result.connection_id,
            columns=[],
            check_issues=False,
        )
        assert "Only one column in schema supported in raw format" in str(binding_response.issues), str(
            binding_response.issues
        )
