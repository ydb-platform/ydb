#!/usr/bin/env python
# -*- coding: utf-8 -*-

import boto3
import json
import logging
import pytest
import typing
import ydb
import ydb.public.api.protos.ydb_value_pb2 as ydb_pb
from hamcrest import assert_that, calling, raises
from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase
from ydb.tests.tools.fq_runner.fq_client import FederatedQueryClient
from ydb.tests.tools.fq_runner.kikimr_utils import yq_all, yq_v1, yq_v2


def to_str(value: typing.Union[bytes, int]) -> str:
    return str(value, 'utf-8') if type(value) is bytes else str(value)


def to_csv(rows: typing.List[typing.Dict[str, typing.Any]]) -> str:
    if len(rows) == 0:
        return ""
    keys = list(rows[0].keys())
    lines = [",".join(keys)]
    lines.extend(",".join(to_str(row[key]) for key in keys) for row in rows)
    return "\n".join(lines)


def assert_dicts(lhs: typing.Dict[str, typing.Any], rhs: typing.Dict[str, typing.Any]):
    for key, val in lhs.items():
        assert val == rhs[key]
    assert len(lhs) == len(rhs)


class TestYdbOverFq(TestYdsBase):
    def make_binding(self, client: FederatedQueryClient, name: str, path: str, connection_id: str, columns: typing.List[typing.Tuple[str, str]]):
        columns = [ydb_pb.Column(name=name, type=ydb_pb.Type(type_id=ydb_pb.Type.PrimitiveTypeId.Value(type))) for name, type in columns]
        client.create_object_storage_binding(name, path, "csv_with_names", connection_id, columns=columns)

    def make_yq_driver(self, endpoint: str, folder_id: str, token: str) -> ydb.Driver:
        config = ydb.DriverConfig(endpoint=endpoint, database="/" + folder_id, auth_token=token)
        driver = ydb.Driver(config)
        try:
            driver.wait(5)
        except TimeoutError:
            logging.error("Failed to create driver for FQ CP. Last reported errors by discovery: "
                          + driver.discovery_debug_details())
        return driver

    def make_s3_client(self, s3):
        resource = boto3.resource(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("fbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        return boto3.client(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

    def put_s3_object(self, client, rows: typing.List[typing.Dict[str, typing.Any]], path: str):
        client.put_object(Body=to_csv(rows), Bucket="fbucket", Key=path, ContentType="text/plain")

    """
    Since this test relies on being isolated (all the tables should be created by it exclusively),
        we want to run each instance (v1/v2) in a separate yq folder. Folders should also
        be separate by yq version
    """
    def list_directory_test_body(self, kikimr, s3, client):
        kikimr.control_plane.wait_bootstrap()

        driver = self.make_yq_driver(kikimr.endpoint(), client.folder_id, "root@builtin")

        # empty result
        ls_res = driver.scheme_client.list_directory("/")
        assert ls_res.is_directory()
        assert len(ls_res.children) == 0

        fruits = [{"Fruit": b"Banana", "Price": 3, "Weight": 100}]
        columns = [("Fruit", "STRING"), ("Price", "INT32"), ("Weight", "INT32")]
        self.put_s3_object(self.make_s3_client(s3), fruits, "fruits.csv")

        connection_id = client.create_storage_connection("fruitbucket", "fbucket").result.connection_id
        self.make_binding(client, "bind0000", "fruits.csv", connection_id, columns)

        # 1 result
        ls_res = driver.scheme_client.list_directory("/")
        assert len(ls_res.children) == 1
        assert ls_res.children[0].name == "bind0000"

        # internally we process 1000 entries at a time, here we test that it goes well
        for i in range(1, 1002):
            self.make_binding(client, "bind{:04}".format(i), "fruits.csv", connection_id, columns)

        ls_res = driver.scheme_client.list_directory("/")
        bindings = sorted([*ls_res.children], key=lambda binding: binding.name)
        assert len(bindings) == 1002
        for i in range(1002):
            assert bindings[i].name == "bind{:04}".format(i)
            assert bindings[i].owner == "root@builtin"
            assert bindings[i].type == ydb.scheme.SchemeEntryType.TABLE

    @yq_v2
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder_v2"}], indirect=True)
    def test_list_directory_v2(self, kikimr, s3, client):
        self.list_directory_test_body(kikimr, s3, client)

    @yq_v1
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder_v1"}], indirect=True)
    def test_list_directory_v1(self, kikimr, s3, client):
        self.list_directory_test_body(kikimr, s3, client)

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_execute_data_query(self, kikimr, s3, client, unique_prefix, yq_version):
        fruits = [
            {"Fruit": b"Banana", "Price": 3, "Weight": 100},
            {"Fruit": b"Apple", "Price": 2, "Weight": 22},
            {"Fruit": b"Pear", "Price": 15, "Weight": 33},
        ]

        self.put_s3_object(self.make_s3_client(s3), fruits, "fruits.csv")

        kikimr.control_plane.wait_bootstrap()
        connection_id = client.create_storage_connection(unique_prefix + "fruitbucket", "fbucket").result.connection_id
        bind_name = unique_prefix + "fruits_bind"
        self.make_binding(client, bind_name, "fruits.csv", connection_id, [("Fruit", "STRING"), ("Price", "INT32"), ("Weight", "INT32")])

        driver = self.make_yq_driver(kikimr.endpoint(), client.folder_id, "root@builtin")
        session = driver.table_client.session().create()
        with session.transaction() as tx:
            query = "select * from {}{}".format("bindings." if yq_version == "v1" else "", bind_name)
            result_set = tx.execute(query)[0]
            for res_row, expected_row in zip(result_set.rows, fruits):
                assert res_row == expected_row

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_execute_data_query_results(self, kikimr, s3, client,):
        kikimr.control_plane.wait_bootstrap()

        driver = self.make_yq_driver(kikimr.endpoint(), client.folder_id, "root@builtin")
        session = driver.table_client.session().create()
        with session.transaction() as tx:
            result_sets = tx.execute("select 42")
            assert len(result_sets) == 1
            assert result_sets[0].rows[0]["column0"] == 42

            result_sets = tx.execute("select 41; select 42; select 43")
            assert len(result_sets) == 3
            for result_set, result in zip(result_sets, [41, 42, 43]):
                assert len(result_set.rows) == 1
                assert result_set.rows[0]["column0"] == result

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_execute_data_query_error(self, kikimr, s3, client, unique_prefix, yq_version):
        kikimr.control_plane.wait_bootstrap()

        fruits = [
            {"Fruit": b"Banana", "Price": 3, "Weight": 100},
            {"Fruit": b"Apple", "Price": 2, "Weight": "WRONG-TYPE"},
            {"Fruit": b"Pear", "Price": 15, "Weight": 33},
        ]
        self.put_s3_object(self.make_s3_client(s3), fruits, "fruits.csv")

        connection_id = client.create_storage_connection(unique_prefix + "fruitbucket", "fbucket").result.connection_id
        bind_name = unique_prefix + "fruits_bind"
        self.make_binding(client, bind_name, "fruits.csv", connection_id, [("Fruit", "STRING"), ("Price", "INT32"), ("Weight", "INT32")])

        driver = self.make_yq_driver(kikimr.endpoint(), client.folder_id, "root@builtin")
        session = driver.table_client.session().create()
        with session.transaction() as tx:
            assert_that(
                calling(tx.execute).with_args(""),
                raises(ydb.issues.InternalError, "length is not in \\[1; 102400\\]"),
            )
        with session.transaction() as tx:
            assert_that(
                calling(tx.execute).with_args("BAD QUERY"),
                raises(ydb.issues.InternalError, "Unexpected token .* : cannot match to any predicted input"),
            )
        with session.transaction() as tx:
            query = "select * from {}{}".format("bindings." if yq_version == "v1" else "", "WRONG_BIND")
            error_pattern = "Table binding `WRONG_BIND` is not defined" if yq_version == "v1" else "Cannot find table"
            assert_that(
                calling(tx.execute).with_args(query),
                raises(ydb.issues.InternalError, error_pattern),
            )
        with session.transaction() as tx:
            query = "select * from {}{}".format("bindings." if yq_version == "v1" else "", bind_name)
            assert_that(
                calling(tx.execute).with_args(query),
                raises(ydb.issues.InternalError, "Error while reading file"),
            )

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_explain_data_query(self, kikimr, s3, client, unique_prefix, yq_version):
        fruits = [{"Fruit": b"Banana", "Price": 3, "Weight": 100}]
        columns = [("Fruit", "STRING"), ("Price", "INT32"), ("Weight", "INT32")]

        self.put_s3_object(self.make_s3_client(s3), fruits, "fruits.csv")

        kikimr.control_plane.wait_bootstrap()
        connection_id = client.create_storage_connection(unique_prefix + "fruitbucket", "fbucket").result.connection_id
        bind_name = unique_prefix + "fruits_bind"
        self.make_binding(client, bind_name, "fruits.csv", connection_id, columns)

        driver = self.make_yq_driver(kikimr.endpoint(), client.folder_id, "root@builtin")
        session = driver.table_client.session().create()

        query = "select * from {}{}".format("bindings." if yq_version == "v1" else "", bind_name)
        explanation = session.explain(query)
        assert len(explanation.query_ast) != 0
        assert json.loads(explanation.query_plan) is not None  # checking it doesn't throw

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_describe_table(self, kikimr, s3, client, unique_prefix):
        fruits = [{"Fruit": b"Banana", "Price": 3, "Weight": 100}]
        columns = [("Fruit", "STRING"), ("Price", "INT32"), ("Weight", "INT32")]

        self.put_s3_object(self.make_s3_client(s3), fruits, "fruits.csv")

        kikimr.control_plane.wait_bootstrap()
        connection_id = client.create_storage_connection(unique_prefix + "fruitbucket", "fbucket").result.connection_id
        bind_name = unique_prefix + "fruits_bind"
        self.make_binding(client, bind_name, "fruits.csv", connection_id, columns)

        driver = self.make_yq_driver(kikimr.endpoint(), client.folder_id, "root@builtin")
        session = driver.table_client.session().create()

        assert_that(
            calling(session.describe_table).with_args("BAD_PATH"),
            # didn't manage to make it find "couldn\'t"
            raises(ydb.issues.NotFound, " find binding with matching name for BAD_PATH")
        )

        for path in [bind_name, "/path/to/" + bind_name]:
            description = session.describe_table(path)
            assert description.name == bind_name
            assert description.type == ydb.scheme.SchemeEntryType.TABLE
            for column in description.columns:
                if column.name == "Fruit":
                    assert column.type == ydb.PrimitiveType.String
                elif column.name == "Price" or column.name == "Weight":
                    assert column.type == ydb.PrimitiveType.Int32
                else:
                    assert False
