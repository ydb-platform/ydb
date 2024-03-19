#!/usr/bin/env python
# -*- coding: utf-8 -*-

import boto3
import logging
import pytest
import typing
import ydb
import ydb.public.api.protos.ydb_value_pb2 as ydb_pb
from hamcrest import assert_that, calling, raises
from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase
from ydb.tests.tools.fq_runner.fq_client import FederatedQueryClient
from ydb.tests.tools.fq_runner.kikimr_utils import yq_v2


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
    @yq_v2
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_execute_data_query(self, kikimr, s3, client):
        fruits = [
            {"Fruit": b"Banana", "Price": 3, "Weight": 100},
            {"Fruit": b"Apple", "Price": 2, "Weight": 22},
            {"Fruit": b"Pear", "Price": 15, "Weight": 33},
        ]

        self.put_s3_object(self.make_s3_client(s3), fruits, "fruits.csv")

        kikimr.control_plane.wait_bootstrap(1)
        connection_id = client.create_storage_connection("fruitbucket", "fbucket").result.connection_id
        self.make_binding(client, "fruits_bind", "fruits.csv", connection_id, [("Fruit", "STRING"), ("Price", "INT32"), ("Weight", "INT32")])

        driver = self.make_yq_driver(kikimr.endpoint(), client.folder_id, "root@builtin")
        session = driver.table_client.session().create()
        with session.transaction() as tx:
            result_set = tx.execute("select * from fruits_bind")[0]
            for res_row, expected_row in zip(result_set.rows, fruits):
                assert_dicts(res_row, expected_row)

    @yq_v2
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_execute_data_query_no_binding(self, kikimr, s3, client):
        kikimr.control_plane.wait_bootstrap(1)

        driver = self.make_yq_driver(kikimr.endpoint(), client.folder_id, "root@builtin")
        session = driver.table_client.session().create()
        with session.transaction() as tx:
            assert_that(
                calling(tx.execute).with_args("select * from fruits_bind"),
                raises(ydb.issues.InternalError),
            )

    @yq_v2
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_list_directory(self, kikimr, s3, client):
        fruits = [{"Fruit": b"Banana", "Price": 3, "Weight": 100}]
        columns = [("Fruit", "STRING"), ("Price", "INT32"), ("Weight", "INT32")]

        self.put_s3_object(self.make_s3_client(s3), fruits, "fruits.csv")

        kikimr.control_plane.wait_bootstrap(1)
        connection_id = client.create_storage_connection("fruitbucket", "fbucket").result.connection_id

        driver = self.make_yq_driver(kikimr.endpoint(), client.folder_id, "root@builtin")

        # no bindings
        ls_res = driver.scheme_client.list_directory("/")
        assert ls_res.is_directory()
        assert len(ls_res.children) == 0

        self.make_binding(client, "bind0000", "fruits.csv", connection_id, columns)

        ls_res = driver.scheme_client.list_directory("/")
        assert len(ls_res.children) == 1
        assert ls_res.children[0].name == "bind0000"

        for i in range(1, 1002):
            self.make_binding(client, "bind{:04}".format(i), "fruits.csv", connection_id, columns)

        ls_res = driver.scheme_client.list_directory("/")
        bindings = sorted([*ls_res.children], key=lambda binding: binding.name)
        assert len(bindings) == 1002
        for i in range(1002):
            assert bindings[i].name == "bind{:04}".format(i)
            assert bindings[i].owner == "root@builtin"
            assert bindings[i].type == ydb.scheme.SchemeEntryType.TABLE

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
