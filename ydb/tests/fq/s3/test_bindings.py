#!/usr/bin/env python
# -*- coding: utf-8 -*-

import boto3
import logging
import pytest

import ydb.public.api.protos.ydb_value_pb2 as ydb
import ydb.public.api.protos.draft.fq_pb2 as fq
from ydb.tests.tools.fq_runner.kikimr_utils import yq_all
from ydb.tests.tools.fq_runner.fq_client import FederatedQueryClient

ValueByTypeExtractors = {
    ydb.Type.PrimitiveTypeId.INT32: lambda x: x.int32_value,
    ydb.Type.PrimitiveTypeId.UINT64: lambda x: x.uint64_value,
    ydb.Type.PrimitiveTypeId.UTF8: lambda x: x.text_value
}


class TestBindings:

    @staticmethod
    def _preprocess_query(sql: str, yq_version: str) -> str:
        if yq_version == 'v1':
            return f"PRAGMA dq.EnableDqReplicate='true'; PRAGMA dq.MaxTasksPerOperation='110'; {sql}"

        return sql

    @staticmethod
    def _assert_connections(client: FederatedQueryClient, expected_connection_names: list[str]):
        actual_connections = [
            connection.content.name
            for connection in client.list_connections(fq.Acl.Visibility.SCOPE).result.connection]
        assert set(actual_connections) == set(expected_connection_names)

    @staticmethod
    def _assert_bindings(client: FederatedQueryClient, expected_binding_names: list[str]):
        actual_bindings = [
            binding.name
            for binding in client.list_bindings(fq.Acl.Visibility.SCOPE).result.binding]
        assert set(actual_bindings) == set(expected_binding_names)

    @staticmethod
    def _assert_query_results(client: FederatedQueryClient, sql: str, yq_version: str, expected_result_set):
        query_id = client.create_query(
            "simple",
            TestBindings._preprocess_query(sql, yq_version),
            type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        expected_columns = expected_result_set['columns']
        expected_rows = expected_result_set['rows']

        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        assert len(result_set.columns) == len(expected_columns)
        for i, (column_name, column_type_id) in enumerate(expected_columns):
            assert result_set.columns[i].name == column_name
            assert result_set.columns[i].type.type_id == column_type_id

        assert len(result_set.rows) == len(expected_rows)
        for i, row in enumerate(expected_rows):
            for j, expected_value in enumerate(row):
                value_extractor = ValueByTypeExtractors[result_set.columns[j].type.type_id]
                assert value_extractor(
                    result_set.rows[i].items[j]) == expected_value

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    @pytest.mark.parametrize("kikimr_settings", [{"is_replace_if_exists": True}, {"is_replace_if_exists": False}], indirect=True)
    def test_binding_operations(self, kikimr, s3, client: FederatedQueryClient, yq_version, unique_prefix):

        resource = boto3.resource(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("bindbucket")
        bucket.create(ACL='public-read')

        s3_client = boto3.client(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )
        s3_client.put_object(Body=R'''{"a": 42, "b": "text"}''',
                             Bucket='bindbucket',
                             Key='abc.json',
                             ContentType='text/json')

        expected_result_set = {
            'columns': [
                ('a', ydb.Type.PrimitiveTypeId.INT32),
                ('b', ydb.Type.PrimitiveTypeId.UTF8)
            ],
            'rows': [(42, "text")]
        }

        # Test connection creation
        kikimr.control_plane.wait_bootstrap(1)
        storage_connection_name = unique_prefix + "connection_name"
        connection_id = client.create_storage_connection(
            storage_connection_name,
            "bindbucket",
            visibility=fq.Acl.Visibility.SCOPE
        ).result.connection_id

        self._assert_connections(
            client, expected_connection_names=[storage_connection_name])
        self._assert_query_results(
            client,
            fR'''
            SELECT *
            FROM `{storage_connection_name}`.`abc.json` WITH (
                FORMAT="json_each_row",
                SCHEMA (
                a Int32 NOT NULL,
                b Utf8 NOT NULL
            ));''',
            yq_version,
            expected_result_set)

        # Test binding creation
        a_type = ydb.Column(name="a", type=ydb.Type(
            type_id=ydb.Type.PrimitiveTypeId.INT32))
        b_type = ydb.Column(name="b", type=ydb.Type(
            type_id=ydb.Type.PrimitiveTypeId.UTF8))
        storage_binding_name = unique_prefix + "binding_name"
        binding_id = client.create_object_storage_binding(name=storage_binding_name,
                                                          path="abc.json",
                                                          format="json_each_row",
                                                          connection_id=connection_id,
                                                          visibility=fq.Acl.Visibility.SCOPE,
                                                          columns=[a_type, b_type]).result.binding_id

        self._assert_bindings(client, expected_binding_names=[storage_binding_name])
        self._assert_query_results(
            client,
            fR'SELECT * FROM bindings.`{storage_binding_name}`;',
            yq_version,
            expected_result_set)

        # Test binding modification
        new_storage_binding_name = unique_prefix + "new_binding_name"
        client.modify_object_storage_binding(binding_id=binding_id,
                                             name=new_storage_binding_name,
                                             path="abc.json",
                                             format="json_each_row",
                                             connection_id=connection_id,
                                             visibility=fq.Acl.Visibility.SCOPE,
                                             columns=[a_type, b_type]).result

        self._assert_bindings(client, expected_binding_names=[
                              new_storage_binding_name])
        self._assert_query_results(
            client,
            fR'SELECT * FROM bindings.`{new_storage_binding_name}`;',
            yq_version,
            expected_result_set)

        # Test connection modification
        new_storage_connection_name = unique_prefix + "new_connection_name"
        client.modify_object_storage_connection(connection_id,
                                                new_storage_connection_name,
                                                "bindbucket",
                                                visibility=fq.Acl.Visibility.SCOPE)

        self._assert_connections(client, expected_connection_names=[
                                 new_storage_connection_name])
        self._assert_query_results(
            client,
            fR'SELECT * FROM bindings.`{new_storage_binding_name}`;',
            yq_version,
            expected_result_set)
        self._assert_query_results(
            client,
            fR'''
            SELECT *
            FROM `{new_storage_connection_name}`.`abc.json` WITH (
                FORMAT="json_each_row",
                SCHEMA (
                a Int32 NOT NULL,
                b Utf8 NOT NULL
            ));''',
            yq_version,
            expected_result_set)

        # Test binding deletion
        client.delete_binding(binding_id)
        self._assert_bindings(client, expected_binding_names=[])

        # Test connection deletion
        client.delete_connection(connection_id)
        self._assert_connections(client, expected_connection_names=[])

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    @pytest.mark.parametrize("kikimr_settings", [{"is_replace_if_exists": True}, {"is_replace_if_exists": False}], indirect=True)
    def test_modify_connection_with_a_lot_of_bindings(self, kikimr, s3, client: FederatedQueryClient, yq_version, unique_prefix):
        pytest.skip("Tiket: YQ-2972")

        resource = boto3.resource(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("bindbucket")
        bucket.create(ACL='public-read')

        s3_client = boto3.client(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )
        s3_client.put_object(Body=R'''{"a": 42, "b": "text"}''',
                             Bucket='bindbucket',
                             Key='abc.json',
                             ContentType='text/json')

        expected_result_set = {
            'columns': [
                ('count', ydb.Type.PrimitiveTypeId.UINT64)
            ],
            'rows': [(100,)]
        }

        # Test connection creation
        kikimr.control_plane.wait_bootstrap(1)
        storage_connection_name = unique_prefix + "connection_name"
        connection_id = client.create_storage_connection(
            storage_connection_name,
            "bindbucket",
            visibility=fq.Acl.Visibility.SCOPE
        ).result.connection_id

        self._assert_connections(
            client, expected_connection_names=[storage_connection_name])

        # Test binding creation
        a_type = ydb.Column(name="a", type=ydb.Type(
            type_id=ydb.Type.PrimitiveTypeId.INT32))
        b_type = ydb.Column(name="b", type=ydb.Type(
            type_id=ydb.Type.PrimitiveTypeId.UTF8))
        for i in range(100):
            client.create_object_storage_binding(name=f"{unique_prefix}binding_name_{i}",
                                                 path="abc.json",
                                                 format="json_each_row",
                                                 connection_id=connection_id,
                                                 visibility=fq.Acl.Visibility.SCOPE,
                                                 columns=[a_type, b_type])

        self._assert_bindings(client,
                              expected_binding_names=[
                                  f'{unique_prefix}binding_name_{i}'
                                  for i in range(100)
                              ])
        self._assert_query_results(
            client,
            f"SELECT COUNT(*) as count FROM ({' UNION ALL '.join(f'SELECT * FROM bindings.`{unique_prefix}binding_name_{i}`' for i in range(100))})",
            yq_version,
            expected_result_set)

        # Test connection modification
        new_storage_connection_name = unique_prefix + "new_connection_name"
        client.modify_object_storage_connection(connection_id,
                                                new_storage_connection_name,
                                                "bindbucket",
                                                visibility=fq.Acl.Visibility.SCOPE)

        self._assert_connections(client, expected_connection_names=[
                                 new_storage_connection_name])
        self._assert_bindings(client,
                              expected_binding_names=[
                                  f'{unique_prefix}binding_name_{i}'
                                  for i in range(100)
                              ])
        self._assert_query_results(
            client,
            f"SELECT COUNT(*) as count FROM ({' UNION ALL '.join(f'SELECT * FROM bindings.`{unique_prefix}binding_name_{i}`' for i in range(100))})",
            yq_version,
            expected_result_set)

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_name_uniqueness_constraint(self, kikimr, client: FederatedQueryClient, unique_prefix):
        # Test connection & binding creation
        kikimr.control_plane.wait_bootstrap(1)
        storage_connection_name = unique_prefix + "connection_name"
        connection_id = client.create_storage_connection(
            storage_connection_name,
            "bindbucket",
            visibility=fq.Acl.Visibility.SCOPE
        ).result.connection_id

        self._assert_connections(
            client, expected_connection_names=[storage_connection_name])

        # Test binding creation
        a_type = ydb.Column(name="a", type=ydb.Type(
            type_id=ydb.Type.PrimitiveTypeId.INT32))
        b_type = ydb.Column(name="b", type=ydb.Type(
            type_id=ydb.Type.PrimitiveTypeId.UTF8))
        storage_binding_name = unique_prefix + "binding_name"
        binding_id = client.create_object_storage_binding(name=storage_binding_name,
                                                          path="abc.json",
                                                          format="json_each_row",
                                                          connection_id=connection_id,
                                                          visibility=fq.Acl.Visibility.SCOPE,
                                                          columns=[a_type, b_type]).result.binding_id
        self._assert_bindings(client, expected_binding_names=[storage_binding_name])

        # Test connection & binding creation with substring names
        storage_connection_substring_name = unique_prefix + "connection"
        connection_id_substring_name = client.create_storage_connection(
            storage_connection_substring_name,
            "bindbucket",
            visibility=fq.Acl.Visibility.SCOPE
        ).result.connection_id

        self._assert_connections(
            client, expected_connection_names=[storage_connection_name, storage_connection_substring_name])

        # Test binding creation
        storage_binding_name_substring = unique_prefix + "binding"
        client.create_object_storage_binding(name=storage_binding_name_substring,
                                             path="abc.json",
                                             format="json_each_row",
                                             connection_id=connection_id_substring_name,
                                             visibility=fq.Acl.Visibility.SCOPE,
                                             columns=[a_type, b_type])
        self._assert_bindings(client, expected_binding_names=[storage_binding_name, storage_binding_name_substring])

        # Test uniqueness constraint
        create_connection_result = client.create_storage_connection(
            storage_binding_name,
            "bindbucket",
            visibility=fq.Acl.Visibility.SCOPE,
            check_issues=False
        )
        assert len(create_connection_result.issues) == 1
        assert create_connection_result.issues[0].message == \
               "Binding with the same name already exists. Please choose another name"
        assert create_connection_result.issues[0].severity == 1

        create_binding_result = client.create_object_storage_binding(name=storage_connection_name,
                                                                     path="abc.json",
                                                                     format="json_each_row",
                                                                     connection_id=connection_id,
                                                                     visibility=fq.Acl.Visibility.SCOPE,
                                                                     columns=[a_type, b_type],
                                                                     check_issues=False)
        assert len(create_binding_result.issues) == 1
        assert create_binding_result.issues[0].message == \
               "Connection with the same name already exists. Please choose another name"
        assert create_binding_result.issues[0].severity == 1

        modify_connection_result = client.modify_object_storage_connection(
            connection_id,
            storage_binding_name,
            "bindbucket",
            visibility=fq.Acl.Visibility.SCOPE,
            check_issues=False
        )
        assert len(modify_connection_result.issues) == 1
        assert modify_connection_result.issues[0].message == \
               "Binding with the same name already exists. Please choose another name"
        assert modify_connection_result.issues[0].severity == 1

        modify_binding_result = client.modify_object_storage_binding(binding_id,
                                                                     name=storage_connection_name,
                                                                     path="abc.json",
                                                                     format="json_each_row",
                                                                     connection_id=connection_id,
                                                                     visibility=fq.Acl.Visibility.SCOPE,
                                                                     columns=[a_type, b_type],
                                                                     check_issues=False)
        assert len(modify_binding_result.issues) == 1
        assert modify_binding_result.issues[0].message == \
               "Connection with the same name already exists. Please choose another name"
        assert modify_binding_result.issues[0].severity == 1

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    @pytest.mark.parametrize("kikimr_settings", [{"bindings_mode": "BM_DROP_WITH_WARNING"}], indirect=True)
    def test_s3_insert(self, kikimr, s3, client, yq_version, unique_prefix):

        resource = boto3.resource(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("bindbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        kikimr.control_plane.wait_bootstrap(1)
        connection_id = client.create_storage_connection(unique_prefix + "bb", "bindbucket").result.connection_id

        fooType = ydb.Column(name="foo", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT32))
        barType = ydb.Column(name="bar", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.UTF8))
        storage_binding_name = unique_prefix + "s3binding"
        client.create_object_storage_binding(name=storage_binding_name,
                                             path="path1/",
                                             format="csv_with_names",
                                             connection_id=connection_id,
                                             columns=[fooType, barType])

        sql = fR'''
            insert into bindings.`{storage_binding_name}`
            select * from AS_TABLE([<|foo:123, bar:"xxx"u|>,<|foo:456, bar:"yyy"u|>]);
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
        if yq_version == "v2":
            issues = str(client.describe_query(query_id).result.query.issue)
            assert "message: \"Please remove \\\'bindings.\\\' from your query, the support for this syntax will be dropped soon" in issues
            assert "severity: 2" in issues

        sql = fR'''
            select foo, bar from bindings.`{storage_binding_name}`;
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
        if yq_version == "v2":
            issues = str(client.describe_query(query_id).result.query.issue)
            assert "message: \"Please remove \\\'bindings.\\\' from your query, the support for this syntax will be dropped soon" in issues
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
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("bindbucket")
        bucket.create(ACL='public-read')

        kikimr.control_plane.wait_bootstrap(1)
        connection_id = client.create_storage_connection(unique_prefix + "bb", "bindbucket").result.connection_id

        fooType = ydb.Column(name="foo", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.UTF8))
        barType = ydb.Column(name="bar", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT32))
        storage_binding_name = unique_prefix + "s3binding"
        client.create_object_storage_binding(name=storage_binding_name,
                                             path="path2/",
                                             format="csv_with_names",
                                             connection_id=connection_id,
                                             columns=[fooType, barType])

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
        client.create_object_storage_binding(name=storage_binding_name,
                                             path="a/",
                                             format="csv_with_names",
                                             connection_id=connection_response.result.connection_id,
                                             columns=[fruitType, priceType],
                                             format_setting={
                                                 "file_pattern": "*.csv"
                                             })

        sql = fR'''
            SELECT *
            FROM bindings.{storage_binding_name};
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS,
                                       pg_syntax=True).result.query_id
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
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("count_for_pg_binding")
        bucket.create(ACL='public-read')

        s3_client = boto3.client(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        row = R'''{"a": 42, "b": 3.14, "c": "text"}'''
        s3_client.put_object(Body=row, Bucket='count_for_pg_binding', Key='abc.json', ContentType='text/json')

        kikimr.control_plane.wait_bootstrap(1)
        connection_response = client.create_storage_connection(unique_prefix + "abc", "count_for_pg_binding")

        aType = ydb.Column(name="a", type=ydb.Type(pg_type=ydb.PgType(oid=23)))
        bType = ydb.Column(name="b", type=ydb.Type(pg_type=ydb.PgType(oid=701)))
        cType = ydb.Column(name="c", type=ydb.Type(pg_type=ydb.PgType(oid=25)))
        storage_binding_name = unique_prefix + "binding_for_count"
        client.create_object_storage_binding(name=storage_binding_name,
                                             path="abc.json",
                                             format="json_each_row",
                                             connection_id=connection_response.result.connection_id,
                                             columns=[aType, bType, cType])

        sql = fR'''
            SELECT COUNT(*)
            FROM bindings.{storage_binding_name};
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS,
                                       pg_syntax=pg_syntax).result.query_id
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
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("bindbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        kikimr.control_plane.wait_bootstrap(1)
        connection_id = client.create_storage_connection(unique_prefix + "bb", "bindbucket").result.connection_id

        data_column = ydb.Column(name="data", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING))
        storage_binding_name = unique_prefix + "s3binding"
        client.create_object_storage_binding(name=storage_binding_name,
                                             path="/",
                                             format="raw",
                                             connection_id=connection_id,
                                             columns=[data_column])

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
        binding_response = client.create_object_storage_binding(name=unique_prefix + "my_binding",
                                                                path="fruits.csv",
                                                                format="raw",
                                                                connection_id=connection_response.result.connection_id,
                                                                columns=[],
                                                                check_issues=False)
        assert "Only one column in schema supported in raw format" in str(binding_response.issues), str(
            binding_response.issues)
