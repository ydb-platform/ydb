#!/usr/bin/env python
# -*- coding: utf-8 -*-

import boto3
import io
import json
import logging
import yatest

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase

import ydb.public.api.protos.ydb_value_pb2 as ydb
import ydb.public.api.protos.draft.fq_pb2 as fq

from ydb.tests.tools.fq_runner.kikimr_utils import yq_all, yq_v2
import ydb.tests.fq.s3.s3_helpers as s3_helpers
import ydb.tests.library.common.yatest_common as yatest_common

from datetime import datetime
from google.protobuf import struct_pb2


class TestS3(TestYdsBase):
    def create_bucket_and_upload_file(self, filename, s3, kikimr):
        s3_helpers.create_bucket_and_upload_file(filename, s3.s3_url, "fbucket", "ydb/tests/fq/s3/test_format_settings")
        kikimr.control_plane.wait_bootstrap(1)

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_interval_unit(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("fbucket")
        bucket.create(ACL='public-read')

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        fruits = R'''Fruit;Price;Duration
Banana;3;100
Apple;2;22
Pear;15;33'''
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='fruits.csv', ContentType='text/plain')
        kikimr.control_plane.wait_bootstrap(1)
        connection_response = client.create_storage_connection(unique_prefix + "fruitbucket", "fbucket")

        fruitType = ydb.Column(name="Fruit", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING))
        priceType = ydb.Column(name="Price", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT32))
        intervalType = ydb.Column(name="Duration", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INTERVAL))
        storage_binding_name = unique_prefix + "my_binding"
        client.create_object_storage_binding(
            name=storage_binding_name,
            path="fruits.csv",
            format="csv_with_names",
            connection_id=connection_response.result.connection_id,
            columns=[fruitType, priceType, intervalType],
            format_setting={"data.interval.unit": "SECONDS", "csv_delimiter": ";"},
        )

        sql = fR'''
            SELECT *
            FROM bindings.`{storage_binding_name}`;
            '''

        query_id = client.create_query("simple", sql).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        logging.debug(str(result_set))
        assert len(result_set.columns) == 3
        assert result_set.columns[0].name == "Duration"
        assert result_set.columns[0].type.type_id == ydb.Type.INTERVAL
        assert result_set.columns[1].name == "Fruit"
        assert result_set.columns[1].type.type_id == ydb.Type.STRING
        assert result_set.columns[2].name == "Price"
        assert result_set.columns[2].type.type_id == ydb.Type.INT32
        assert len(result_set.rows) == 3
        assert result_set.rows[0].items[0].int64_value == 100000000
        assert result_set.rows[0].items[1].bytes_value == b"Banana"
        assert result_set.rows[0].items[2].int32_value == 3
        assert result_set.rows[1].items[0].int64_value == 22000000
        assert result_set.rows[1].items[1].bytes_value == b"Apple"
        assert result_set.rows[1].items[2].int32_value == 2
        assert result_set.rows[2].items[0].int64_value == 33000000
        assert result_set.rows[2].items[1].bytes_value == b"Pear"
        assert result_set.rows[2].items[2].int32_value == 15

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_bad_format_setting(self, kikimr, client, unique_prefix):
        kikimr.control_plane.wait_bootstrap(1)
        connection_response = client.create_storage_connection(unique_prefix + "fruitbucket", "fbucket")

        fruitType = ydb.Column(name="Fruit", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING))
        priceType = ydb.Column(name="Price", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT32))
        intervalType = ydb.Column(name="Duration", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INTERVAL))
        binding_response = client.create_object_storage_binding(
            name=unique_prefix + "my_binding",
            path="fruits.csv",
            format="csv_with_names",
            connection_id=connection_response.result.connection_id,
            columns=[fruitType, priceType, intervalType],
            format_setting={"data.interval.unit": "SEKUNDA"},
            check_issues=False,
        )
        assert "unknown value for data.interval.unit SEKUNDA" in str(binding_response.issues), str(
            binding_response.issues
        )

    def validate_timestamp_iso_result(self, result_set):
        logging.debug(str(result_set))
        assert len(result_set.columns) == 4
        assert result_set.columns[0].name == "Fruit"
        assert result_set.columns[0].type.type_id == ydb.Type.STRING
        assert result_set.columns[1].name == "Price"
        assert result_set.columns[1].type.type_id == ydb.Type.INT32
        assert result_set.columns[2].name == "Time"
        assert result_set.columns[2].type.type_id == ydb.Type.TIMESTAMP
        assert result_set.columns[3].name == "Weight"
        assert result_set.columns[3].type.type_id == ydb.Type.INT32

        assert len(result_set.rows) == 4
        assert result_set.rows[0].items[0].bytes_value == b"Banana"
        assert result_set.rows[0].items[1].int32_value == 3
        assert result_set.rows[0].items[2].uint64_value == 1666197647218000
        assert result_set.rows[0].items[3].int32_value == 100

        assert result_set.rows[1].items[0].bytes_value == b"Apple"
        assert result_set.rows[1].items[1].int32_value == 2
        assert result_set.rows[1].items[2].uint64_value == 1666186847000000
        assert result_set.rows[1].items[3].int32_value == 22

        assert result_set.rows[2].items[0].bytes_value == b"Pear"
        assert result_set.rows[2].items[1].int32_value == 15
        assert result_set.rows[2].items[2].uint64_value == 1666197647000000
        assert result_set.rows[2].items[3].int32_value == 33

        assert result_set.rows[3].items[0].bytes_value == b"Orange"
        assert result_set.rows[3].items[1].int32_value == 1
        assert result_set.rows[3].items[2].uint64_value == 1666197647218000
        assert result_set.rows[3].items[3].int32_value == 2

    def validate_timestamp_posix_result(self, result_set):
        logging.debug(str(result_set))
        assert len(result_set.columns) == 4
        assert result_set.columns[0].name == "Fruit"
        assert result_set.columns[0].type.type_id == ydb.Type.STRING
        assert result_set.columns[1].name == "Price"
        assert result_set.columns[1].type.type_id == ydb.Type.INT32
        assert result_set.columns[2].name == "Time"
        assert result_set.columns[2].type.type_id == ydb.Type.TIMESTAMP
        assert result_set.columns[3].name == "Weight"
        assert result_set.columns[3].type.type_id == ydb.Type.INT32

        assert len(result_set.rows) == 2
        assert result_set.rows[0].items[0].bytes_value == b"Banana"
        assert result_set.rows[0].items[1].int32_value == 3
        assert result_set.rows[0].items[2].uint64_value == 1666197647000000
        assert result_set.rows[0].items[3].int32_value == 100

        assert result_set.rows[1].items[0].bytes_value == b"Apple"
        assert result_set.rows[1].items[1].int32_value == 2
        assert result_set.rows[1].items[2].uint64_value == 1666197707000000
        assert result_set.rows[1].items[3].int32_value == 22

    def validate_date_time_iso_result(self, result_set):
        logging.debug(str(result_set))
        assert len(result_set.columns) == 4
        assert result_set.columns[0].name == "Fruit"
        assert result_set.columns[0].type.type_id == ydb.Type.STRING
        assert result_set.columns[1].name == "Price"
        assert result_set.columns[1].type.type_id == ydb.Type.INT32
        assert result_set.columns[2].name == "Time"
        assert result_set.columns[2].type.type_id == ydb.Type.DATETIME
        assert result_set.columns[3].name == "Weight"
        assert result_set.columns[3].type.type_id == ydb.Type.INT32

        assert len(result_set.rows) == 3
        assert result_set.rows[0].items[0].bytes_value == b"Banana"
        assert result_set.rows[0].items[1].int32_value == 3
        assert result_set.rows[0].items[2].uint32_value == 1666197647
        assert result_set.rows[0].items[3].int32_value == 100

        assert result_set.rows[1].items[0].bytes_value == b"Apple"
        assert result_set.rows[1].items[1].int32_value == 2
        assert result_set.rows[1].items[2].uint32_value == 1666186847
        assert result_set.rows[1].items[3].int32_value == 22

        assert result_set.rows[2].items[0].bytes_value == b"Pear"
        assert result_set.rows[2].items[1].int32_value == 15
        assert result_set.rows[2].items[2].uint32_value == 1666197647
        assert result_set.rows[2].items[3].int32_value == 33

    def validate_date_time_posix_result(self, result_set):
        logging.debug(str(result_set))
        assert len(result_set.columns) == 4
        assert result_set.columns[0].name == "Fruit"
        assert result_set.columns[0].type.type_id == ydb.Type.STRING
        assert result_set.columns[1].name == "Price"
        assert result_set.columns[1].type.type_id == ydb.Type.INT32
        assert result_set.columns[2].name == "Time"
        assert result_set.columns[2].type.type_id == ydb.Type.DATETIME
        assert result_set.columns[3].name == "Weight"
        assert result_set.columns[3].type.type_id == ydb.Type.INT32

        assert len(result_set.rows) == 2
        assert result_set.rows[0].items[0].bytes_value == b"Banana"
        assert result_set.rows[0].items[1].int32_value == 3
        assert result_set.rows[0].items[2].uint32_value == 1666197647
        assert result_set.rows[0].items[3].int32_value == 100

        assert result_set.rows[1].items[0].bytes_value == b"Apple"
        assert result_set.rows[1].items[1].int32_value == 2
        assert result_set.rows[1].items[2].uint32_value == 1666197707
        assert result_set.rows[1].items[3].int32_value == 22

    def canonize_result(self, s3, s3_path, filename):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("fbucket")

        for s3_object in bucket.objects.all():
            if s3_object.key.startswith(s3_path):
                bytes_buffer = io.BytesIO()
                bucket.download_fileobj(s3_object.key, bytes_buffer)
                byte_value = bytes_buffer.getvalue()
                str_value = byte_value.decode('utf-8', errors='ignore')

                canonical_path = yatest.common.work_path(filename)
                with open(canonical_path, "w") as f:
                    f.write(str_value)
                return yatest.common.canonical_file(canonical_path, local=True)

    def create_source_timestamp_binding(
        self, unique_prefix, client, connection_id, filename, type_format, format_name=None, format=None
    ):
        timeType = ydb.Column(name="Time", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.TIMESTAMP))
        fruitType = ydb.Column(name="Fruit", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING))
        priceType = ydb.Column(name="Price", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT32))
        weightType = ydb.Column(name="Weight", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT32))
        if format_name:
            format_setting = {"data.timestamp.format_name": format_name}
        else:
            format_setting = {"data.timestamp.format": format}
        storage_binding_name = unique_prefix + "my_binding"
        client.create_object_storage_binding(
            name=storage_binding_name,
            path=filename,
            format=type_format,
            connection_id=connection_id,
            columns=[timeType, fruitType, priceType, weightType],
            format_setting=format_setting,
        )
        return storage_binding_name

    def create_sink_timestamp_binding(
        self, unique_prefix, client, connection_id, prefix, type_format, format_name=None, format=None
    ):
        timeType = ydb.Column(name="Time", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.TIMESTAMP))
        fruitType = ydb.Column(name="Fruit", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING))
        priceType = ydb.Column(name="Price", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT32))
        weightType = ydb.Column(name="Weight", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT32))
        if format_name:
            format_setting = {"data.timestamp.format_name": format_name}
        else:
            format_setting = {"data.timestamp.format": format}
        storage_binding_name = unique_prefix + "insert_my_binding"
        client.create_object_storage_binding(
            name=storage_binding_name,
            path=prefix,
            format=type_format,
            connection_id=connection_id,
            columns=[timeType, fruitType, priceType, weightType],
            format_setting=format_setting,
        )
        return storage_binding_name

    def create_source_date_time_binding(
        self, unique_prefix, client, connection_id, filename, type_format, format_name=None, format=None
    ):
        timeType = ydb.Column(name="Time", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.DATETIME))
        fruitType = ydb.Column(name="Fruit", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING))
        priceType = ydb.Column(name="Price", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT32))
        weightType = ydb.Column(name="Weight", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT32))
        if format_name:
            format_setting = {"data.datetime.format_name": format_name}
        else:
            format_setting = {"data.datetime.format": format}
        storage_binding_name = unique_prefix + "my_binding"
        client.create_object_storage_binding(
            name=storage_binding_name,
            path=filename,
            format=type_format,
            connection_id=connection_id,
            columns=[timeType, fruitType, priceType, weightType],
            format_setting=format_setting,
        )
        return storage_binding_name

    def create_sink_date_time_binding(
        self, unique_prefix, client, connection_id, prefix, type_format, format_name=None, format=None
    ):
        timeType = ydb.Column(name="Time", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.DATETIME))
        fruitType = ydb.Column(name="Fruit", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING))
        priceType = ydb.Column(name="Price", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT32))
        weightType = ydb.Column(name="Weight", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT32))
        if format_name:
            format_setting = {"data.datetime.format_name": format_name}
        else:
            format_setting = {"data.datetime.format": format}
        storage_binding_name = unique_prefix + "insert_my_binding"
        client.create_object_storage_binding(
            name=storage_binding_name,
            path=prefix,
            format=type_format,
            connection_id=connection_id,
            columns=[timeType, fruitType, priceType, weightType],
            format_setting=format_setting,
        )
        return storage_binding_name

    @yq_all
    @pytest.mark.parametrize(
        "filename, type_format",
        [
            ("timestamp/simple_iso/test.csv", "csv_with_names"),
            ("timestamp/simple_iso/test.tsv", "tsv_with_names"),
            ("timestamp/simple_iso/test.json", "json_each_row"),
            ("timestamp/simple_iso/test.parquet", "parquet"),
        ],
    )
    def test_timestamp_simple_iso(self, kikimr, s3, client, filename, type_format, unique_prefix):
        self.create_bucket_and_upload_file(filename, s3, kikimr)
        connection_response = client.create_storage_connection(unique_prefix + "fruitbucket", "fbucket")

        storage_source_binding_name = self.create_source_timestamp_binding(
            unique_prefix, client, connection_response.result.connection_id, filename, type_format, "ISO"
        )

        sql = f'''
            SELECT *
            FROM bindings.`{storage_source_binding_name}`
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        self.validate_timestamp_iso_result(result_set)

    @yq_all
    @pytest.mark.parametrize(
        "filename, type_format",
        [
            ("timestamp/simple_iso/test.csv", "csv_with_names"),
            ("timestamp/simple_iso/test.tsv", "tsv_with_names"),
            ("timestamp/simple_iso/test.json", "json_each_row"),
            ("timestamp/simple_iso/test.parquet", "parquet"),
        ],
    )
    def test_timestamp_simple_iso_insert(self, kikimr, s3, client, filename, type_format, unique_prefix):
        self.create_bucket_and_upload_file(filename, s3, kikimr)
        connection_response = client.create_storage_connection(unique_prefix + "fruitbucket", "fbucket")

        storage_source_binding_name = self.create_source_timestamp_binding(
            unique_prefix, client, connection_response.result.connection_id, filename, type_format, "ISO"
        )
        storage_sink_binding_name = self.create_sink_timestamp_binding(
            unique_prefix,
            client,
            connection_response.result.connection_id,
            "timestamp/simple_iso/" + type_format + "/",
            type_format,
            "ISO",
        )

        sql = f'''
            INSERT INTO bindings.`{storage_sink_binding_name}`
            SELECT Unwrap(Time + Interval("P1D")) as Time, Fruit, Price, Weight
            FROM bindings.`{storage_source_binding_name}`
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        return self.canonize_result(s3, "timestamp/simple_iso/" + type_format + "/", filename.replace('/', '_'))

    @yq_all
    @pytest.mark.parametrize(
        "filename, type_format",
        [
            ("common/simple_posix/test.csv", "csv_with_names"),
            ("common/simple_posix/test.tsv", "tsv_with_names"),
            ("common/simple_posix/test.json", "json_each_row"),
            ("common/simple_posix/test.parquet", "parquet"),
        ],
    )
    def test_timestamp_simple_posix(self, kikimr, s3, client, filename, type_format, unique_prefix):
        self.create_bucket_and_upload_file(filename, s3, kikimr)
        connection_response = client.create_storage_connection(unique_prefix + "fruitbucket", "fbucket")

        storage_source_binding_name = self.create_source_timestamp_binding(
            unique_prefix, client, connection_response.result.connection_id, filename, type_format, "POSIX"
        )

        sql = f'''
            SELECT *
            FROM bindings.`{storage_source_binding_name}`
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        self.validate_timestamp_posix_result(result_set)

    @yq_all
    @pytest.mark.parametrize(
        "filename, type_format",
        [
            ("common/simple_posix/test.csv", "csv_with_names"),
            ("common/simple_posix/test.tsv", "tsv_with_names"),
            ("common/simple_posix/test.json", "json_each_row"),
            ("common/simple_posix/test.parquet", "parquet"),
        ],
    )
    def test_timestamp_simple_posix_insert(self, kikimr, s3, client, filename, type_format, unique_prefix):
        self.create_bucket_and_upload_file(filename, s3, kikimr)
        connection_response = client.create_storage_connection(unique_prefix + "fruitbucket", "fbucket")

        storage_source_binding_name = self.create_source_timestamp_binding(
            unique_prefix, client, connection_response.result.connection_id, filename, type_format, "POSIX"
        )
        storage_sink_binding_name = self.create_sink_timestamp_binding(
            unique_prefix,
            client,
            connection_response.result.connection_id,
            "timestamp/simple_posix/" + type_format + "/",
            type_format,
            "POSIX",
        )

        sql = f'''
            INSERT INTO bindings.`{storage_sink_binding_name}`
            SELECT Unwrap(Time + Interval("P1D")) as Time, Fruit, Price, Weight
            FROM bindings.`{storage_source_binding_name}`
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        return self.canonize_result(s3, "timestamp/simple_posix/" + type_format + "/", filename.replace('/', '_'))

    @yq_all
    @pytest.mark.parametrize(
        "filename, type_format",
        [
            ("date_time/simple_iso/test.csv", "csv_with_names"),
            ("date_time/simple_iso/test.tsv", "tsv_with_names"),
            ("date_time/simple_iso/test.json", "json_each_row"),
            ("date_time/simple_iso/test.parquet", "parquet"),
        ],
    )
    def test_date_time_simple_iso(self, kikimr, s3, client, filename, type_format, unique_prefix):
        self.create_bucket_and_upload_file(filename, s3, kikimr)
        connection_response = client.create_storage_connection(unique_prefix + "fruitbucket", "fbucket")

        storage_source_binding_name = self.create_source_date_time_binding(
            unique_prefix, client, connection_response.result.connection_id, filename, type_format, "ISO"
        )

        sql = f'''
            SELECT *
            FROM bindings.`{storage_source_binding_name}`
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        self.validate_date_time_iso_result(result_set)

    @yq_all
    @pytest.mark.parametrize(
        "filename, type_format",
        [
            ("date_time/simple_iso/test.csv", "csv_with_names"),
            ("date_time/simple_iso/test.tsv", "tsv_with_names"),
            ("date_time/simple_iso/test.json", "json_each_row"),
            ("date_time/simple_iso/test.parquet", "parquet"),
        ],
    )
    def test_date_time_simple_iso_insert(self, kikimr, s3, client, filename, type_format, unique_prefix):
        self.create_bucket_and_upload_file(filename, s3, kikimr)
        connection_response = client.create_storage_connection(unique_prefix + "fruitbucket", "fbucket")

        storage_source_binding_name = self.create_source_date_time_binding(
            unique_prefix, client, connection_response.result.connection_id, filename, type_format, "ISO"
        )
        storage_sink_binding_name = self.create_sink_date_time_binding(
            unique_prefix,
            client,
            connection_response.result.connection_id,
            "date_time/simple_iso/" + type_format + "/",
            type_format,
            "ISO",
        )

        sql = f'''
            INSERT INTO bindings.`{storage_sink_binding_name}`
            SELECT Unwrap(Time + Interval("P1D")) as Time, Fruit, Price, Weight
            FROM bindings.`{storage_source_binding_name}`
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        return self.canonize_result(s3, "date_time/simple_iso/" + type_format + "/", filename.replace('/', '_'))

    @yq_all
    @pytest.mark.parametrize(
        "filename, type_format",
        [
            ("common/simple_posix/test.csv", "csv_with_names"),
            ("common/simple_posix/test.tsv", "tsv_with_names"),
            ("common/simple_posix/test.json", "json_each_row"),
            ("common/simple_posix/test.parquet", "parquet"),
        ],
    )
    def test_date_time_simple_posix(self, kikimr, s3, client, filename, type_format, unique_prefix):
        self.create_bucket_and_upload_file(filename, s3, kikimr)
        connection_response = client.create_storage_connection(unique_prefix + "fruitbucket", "fbucket")

        storage_source_binding_name = self.create_source_date_time_binding(
            unique_prefix, client, connection_response.result.connection_id, filename, type_format, "POSIX"
        )

        sql = f'''
            SELECT *
            FROM bindings.`{storage_source_binding_name}`
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        self.validate_date_time_posix_result(result_set)

    @yq_all
    @pytest.mark.parametrize(
        "filename, type_format",
        [
            ("common/simple_posix/test.csv", "csv_with_names"),
            ("common/simple_posix/test.tsv", "tsv_with_names"),
            ("common/simple_posix/test.json", "json_each_row"),
            ("common/simple_posix/test.parquet", "parquet"),
        ],
    )
    def test_date_time_simple_posix_insert(self, kikimr, s3, client, filename, type_format, unique_prefix):
        self.create_bucket_and_upload_file(filename, s3, kikimr)
        connection_response = client.create_storage_connection(unique_prefix + "fruitbucket", "fbucket")

        storage_source_binding_name = self.create_source_date_time_binding(
            unique_prefix, client, connection_response.result.connection_id, filename, type_format, "POSIX"
        )
        storage_sink_binding_name = self.create_sink_date_time_binding(
            unique_prefix,
            client,
            connection_response.result.connection_id,
            "datetime/simple_posix/" + type_format + "/",
            type_format,
            "POSIX",
        )

        sql = f'''
            INSERT INTO bindings.`{storage_sink_binding_name}`
            SELECT Unwrap(Time + Interval("P1D")) as Time, Fruit, Price, Weight
            FROM bindings.`{storage_source_binding_name}`
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        return self.canonize_result(s3, "datetime/simple_posix/" + type_format + "/", filename.replace('/', '_'))

    @yq_all
    @pytest.mark.parametrize(
        "timestamp_format", ["UNIX_TIME_SECONDS", "UNIX_TIME_MICROSECONDS", "UNIX_TIME_MILLISECONDS"]
    )
    @pytest.mark.parametrize(
        "filename, type_format",
        [
            ("timestamp/unix_time/test.csv", "csv_with_names"),
            ("timestamp/unix_time/test.tsv", "tsv_with_names"),
            ("timestamp/unix_time/test.json", "json_each_row"),
            ("timestamp/unix_time/test.parquet", "parquet"),
        ],
    )
    def test_timestamp_unix_time_insert(
        self, kikimr, s3, client, filename, type_format, timestamp_format, unique_prefix
    ):
        self.create_bucket_and_upload_file(filename, s3, kikimr)
        connection_response = client.create_storage_connection(unique_prefix + "fruitbucket", "fbucket")

        storage_source_binding_name = self.create_source_timestamp_binding(
            unique_prefix, client, connection_response.result.connection_id, filename, type_format, timestamp_format
        )
        storage_sink_binding_name = self.create_sink_timestamp_binding(
            unique_prefix,
            client,
            connection_response.result.connection_id,
            "timestamp/unix_time/" + type_format + "/",
            type_format,
            timestamp_format,
        )

        sql = f'''
            INSERT INTO bindings.`{storage_sink_binding_name}`
            SELECT Unwrap(Time + Interval("P1D")) as Time, Fruit, Price, Weight
            FROM bindings.`{storage_source_binding_name}`
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        return self.canonize_result(
            s3, "timestamp/unix_time/" + type_format + "/", timestamp_format + "_" + filename.replace('/', '_')
        )

    @yq_all
    @pytest.mark.parametrize(
        "filename, type_format",
        [
            ("common/simple_format/test.csv", "csv_with_names"),
            ("common/simple_format/test.tsv", "tsv_with_names"),
            ("common/simple_format/test.json", "json_each_row"),
            ("common/simple_format/test.parquet", "parquet"),
        ],
    )
    def test_timestamp_simple_format_insert(self, kikimr, s3, client, filename, type_format, unique_prefix):
        self.create_bucket_and_upload_file(filename, s3, kikimr)
        connection_response = client.create_storage_connection(unique_prefix + "fruitbucket", "fbucket")

        storage_source_binding_name = self.create_source_timestamp_binding(
            unique_prefix, client, connection_response.result.connection_id, filename, type_format, format="%Y-%m-%d"
        )
        storage_sink_binding_name = self.create_sink_timestamp_binding(
            unique_prefix,
            client,
            connection_response.result.connection_id,
            "common/simple_format/" + type_format + "/",
            type_format,
            format="%Y-%m-%d",
        )

        sql = f'''
            INSERT INTO bindings.`{storage_sink_binding_name}`
            SELECT Unwrap(Time + Interval("P1D")) as Time, Fruit, Price, Weight
            FROM bindings.`{storage_source_binding_name}`
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        return self.canonize_result(
            s3, "common/simple_format/" + type_format + "/", "timestamp_format_" + filename.replace('/', '_')
        )

    @yq_all
    @pytest.mark.parametrize(
        "filename, type_format",
        [
            ("common/simple_format/test.csv", "csv_with_names"),
            ("common/simple_format/test.tsv", "tsv_with_names"),
            ("common/simple_format/test.json", "json_each_row"),
            ("common/simple_format/test.parquet", "parquet"),
        ],
    )
    def test_date_time_simple_format_insert(self, kikimr, s3, client, filename, type_format, unique_prefix):
        self.create_bucket_and_upload_file(filename, s3, kikimr)
        connection_response = client.create_storage_connection(unique_prefix + "fruitbucket", "fbucket")

        storage_source_binding_name = self.create_source_date_time_binding(
            unique_prefix, client, connection_response.result.connection_id, filename, type_format, format="%Y-%m-%d"
        )
        storage_sink_binding_name = self.create_sink_date_time_binding(
            unique_prefix,
            client,
            connection_response.result.connection_id,
            "common/simple_format/" + type_format + "/",
            type_format,
            format="%Y-%m-%d",
        )

        sql = f'''
            INSERT INTO bindings.`{storage_sink_binding_name}`
            SELECT Unwrap(Time + Interval("P1D")) as Time, Fruit, Price, Weight
            FROM bindings.`{storage_source_binding_name}`
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        return self.canonize_result(
            s3, "common/simple_format/" + type_format + "/", "date_time_format_" + filename.replace('/', '_')
        )

    @yq_all
    @pytest.mark.parametrize(
        "filename, type_format, format_name",
        [
            ("common/simple_posix/big.csv", "csv_with_names", "POSIX"),
            ("common/simple_format/big.csv", "csv_with_names", "%Y-%m-%d"),
            ("date_time/simple_iso/big.csv", "csv_with_names", "ISO"),
        ],
    )
    def test_date_time_simple_posix_big_file(
        self, kikimr, s3, client, filename, type_format, format_name, unique_prefix
    ):
        self.create_bucket_and_upload_file(filename, s3, kikimr)
        connection_response = client.create_storage_connection(unique_prefix + "fruitbucket", "fbucket")

        a = ydb.Column(name="tpep_pickup_datetime", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.DATETIME))
        b = ydb.Column(name="tpep_dropoff_datetime", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.DATETIME))
        storage_source_binding_name = unique_prefix + "my_binding"
        client.create_object_storage_binding(
            name=storage_source_binding_name,
            path=filename,
            format=type_format,
            connection_id=connection_response.result.connection_id,
            columns=[a, b],
            format_setting={
                (
                    "data.datetime.format"
                    if format_name != "ISO" and format_name != "POSIX"
                    else "data.datetime.format_name"
                ): format_name
            },
        )

        sql = f'''
            SELECT *
            FROM bindings.`{storage_source_binding_name}`
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    @pytest.mark.parametrize("pg_syntax", [False, True], ids=["yql_syntax", "pg_syntax"])
    @pytest.mark.parametrize("pg_types", [False, True], ids=["yql_types", "pg_types"])
    def test_precompute_with_pg_binding(self, kikimr, s3, client, pg_syntax, pg_types, unique_prefix):
        if pg_syntax and not pg_types:
            pytest.skip("pg syntax is only supported with pg types")
        test_suffix = "_{}_{}".format(1 if pg_syntax else 0, 1 if pg_types else 0)
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket_name = "precompute_with_pg_binding" + test_suffix
        bucket = resource.Bucket(bucket_name)
        bucket.create(ACL='public-read')

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        if pg_types:
            idType = ydb.Column(name="id", type=ydb.Type(pg_type=ydb.PgType(oid=23)))
            nameType = ydb.Column(name="name", type=ydb.Type(pg_type=ydb.PgType(oid=25)))
        else:
            idType = ydb.Column(name="id", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT32))
            nameType = ydb.Column(name="name", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING))

        ids_data = R'''{"id": 42}'''

        s3_client.put_object(Body=ids_data, Bucket=bucket_name, Key='ids.json', ContentType='text/json')
        connection_response = client.create_storage_connection(
            unique_prefix + "precompute_with_pg" + test_suffix, bucket_name
        )

        binding_for_ids_name = unique_prefix + "binding_for_ids" + test_suffix
        client.create_object_storage_binding(
            name=binding_for_ids_name,
            path="ids.json",
            format="json_each_row",
            connection_id=connection_response.result.connection_id,
            columns=[idType],
        )

        bucket.create(ACL='public-read')

        names_data = R'''{"id": 42, "name": "hello"}
        {"id": 0, "name": "goodbye"}'''

        s3_client.put_object(Body=names_data, Bucket=bucket_name, Key='names.json', ContentType='text/json')
        binding_for_names_name = unique_prefix + "binding_for_names" + test_suffix
        client.create_object_storage_binding(
            name=binding_for_names_name,
            path="names.json",
            format="json_each_row",
            connection_id=connection_response.result.connection_id,
            columns=[idType, nameType],
        )

        kikimr.control_plane.wait_bootstrap(1)

        sql = R'''
            SELECT *
            FROM bindings.{}
            WHERE id IN (
                SELECT
                    id
                FROM bindings.{}
            )
            '''.format(
            binding_for_names_name, binding_for_ids_name
        )

        query_id = client.create_query(
            "simple", sql, type=fq.QueryContent.QueryType.ANALYTICS, pg_syntax=pg_syntax
        ).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        logging.debug(str(result_set))
        assert len(result_set.columns) == 2
        assert len(result_set.rows) == 1
        if pg_types:
            assert result_set.columns[0].type.pg_type.oid == 23
            assert result_set.columns[1].type.pg_type.oid == 25
            assert result_set.rows[0].items[0].text_value == "42"
            assert result_set.rows[0].items[1].text_value == "hello"
        else:
            assert result_set.columns[0].type.type_id == ydb.Type.INT32
            assert result_set.columns[1].type.type_id == ydb.Type.STRING
            assert result_set.rows[0].items[0].int32_value == 42
            assert result_set.rows[0].items[1].bytes_value == b"hello"

    @yq_all
    @pytest.mark.parametrize("filename, type_format", [("timestamp/completeness_iso/test.csv", "csv_with_names")])
    def test_timestamp_completeness_iso(self, kikimr, s3, client, filename, type_format, unique_prefix):
        self.create_bucket_and_upload_file(filename, s3, kikimr)
        connection_response = client.create_storage_connection(unique_prefix + "fruitbucket", "fbucket")

        storage_source_binding_name = self.create_source_timestamp_binding(
            unique_prefix, client, connection_response.result.connection_id, filename, type_format, "ISO"
        )

        sql = f'''
            SELECT *
            FROM bindings.`{storage_source_binding_name}`
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        data = client.get_result_data(query_id, limit=50)
        result_set = data.result.result_set

        logging.debug(str(result_set))
        assert len(result_set.columns) == 4
        assert result_set.columns[0].name == "Fruit"
        assert result_set.columns[0].type.type_id == ydb.Type.STRING
        assert result_set.columns[1].name == "Price"
        assert result_set.columns[1].type.type_id == ydb.Type.INT32
        assert result_set.columns[2].name == "Time"
        assert result_set.columns[2].type.type_id == ydb.Type.TIMESTAMP
        assert result_set.columns[3].name == "Weight"
        assert result_set.columns[3].type.type_id == ydb.Type.INT32

        assert len(result_set.rows) == 36

    @yq_all
    @pytest.mark.parametrize(
        "filename, type_format",
        [
            ("date_time/completeness_iso/test.csv", "csv_with_names"),
        ],
    )
    def test_date_time_completeness_iso(self, kikimr, s3, client, filename, type_format, unique_prefix):
        self.create_bucket_and_upload_file(filename, s3, kikimr)
        connection_response = client.create_storage_connection(unique_prefix + "fruitbucket", "fbucket")

        storage_source_binding_name = self.create_source_date_time_binding(
            unique_prefix, client, connection_response.result.connection_id, filename, type_format, "ISO"
        )

        sql = f'''
            SELECT *
            FROM bindings.`{storage_source_binding_name}`
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        data = client.get_result_data(query_id, limit=50)
        result_set = data.result.result_set

        logging.debug(str(result_set))
        assert len(result_set.columns) == 4
        assert result_set.columns[0].name == "Fruit"
        assert result_set.columns[0].type.type_id == ydb.Type.STRING
        assert result_set.columns[1].name == "Price"
        assert result_set.columns[1].type.type_id == ydb.Type.INT32
        assert result_set.columns[2].name == "Time"
        assert result_set.columns[2].type.type_id == ydb.Type.DATETIME
        assert result_set.columns[3].name == "Weight"
        assert result_set.columns[3].type.type_id == ydb.Type.INT32

        assert len(result_set.rows) == 6

    @yq_all
    @pytest.mark.parametrize("filename", [("date_null/as_default/test.csv"), ("date_null/parse_error/test.csv")])
    def test_date_null(self, kikimr, s3, client, filename, unique_prefix):
        self.create_bucket_and_upload_file(filename, s3, kikimr)
        storage_connection_name = unique_prefix + "hcpp"
        client.create_storage_connection(storage_connection_name, "fbucket")

        sql = f'''
            SELECT
                `put`
            FROM
                `{storage_connection_name}`.`/{filename}`
            WITH (FORMAT="csv_with_names",
                csv_delimiter=",",
                SCHEMA=(
                `put` Date
                ))
            LIMIT 10;
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
        data = client.get_result_data(query_id, limit=50)
        assert data.result.result_set.rows[0].items[0].null_flag_value == struct_pb2.NULL_VALUE, str(
            data.result.result_set
        )

    @yq_all
    @pytest.mark.parametrize("filename", [("date_null/as_default/test.csv"), ("date_null/parse_error/test.csv")])
    def test_date_null_with_not_null_type(self, kikimr, s3, client, filename, unique_prefix):
        self.create_bucket_and_upload_file(filename, s3, kikimr)
        storage_connection_name = unique_prefix + "hcpp"
        client.create_storage_connection(storage_connection_name, "fbucket")

        sql = f'''
            SELECT
                `put`
            FROM
                `{storage_connection_name}`.`/{filename}`
            WITH (FORMAT="csv_with_names",
                csv_delimiter=",",
                SCHEMA=(
                `put` Date NOT NULL
                ))
            LIMIT 10;
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.FAILED)
        describe_result = client.describe_query(query_id).result
        issues = describe_result.query.issue[0].issues
        assert "Invalid data format" in str(issues), str(describe_result)
        assert "name: put, type: Date, ERROR: text " in str(issues), str(describe_result)
        assert "is not like Date" in str(issues), str(describe_result)

    @yq_all
    @pytest.mark.parametrize(
        "filename", [("date_null/as_default/multi_null.csv"), ("date_null/parse_error/multi_null.csv")]
    )
    def test_date_null_multi(self, kikimr, s3, client, filename, unique_prefix):
        self.create_bucket_and_upload_file(filename, s3, kikimr)
        storage_connection_name = unique_prefix + "hcpp"
        client.create_storage_connection(storage_connection_name, "fbucket")

        sql = f'''
            SELECT
                `put`, `a`, `t`
            FROM
                `{storage_connection_name}`.`/{filename}`
            WITH (FORMAT="csv_with_names",
                csv_delimiter=",",
                SCHEMA=(
                `put` Date,
                `a` Date,
                `t` Date
                ))
            LIMIT 10;
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
        data = client.get_result_data(query_id, limit=50)
        assert data.result.result_set.rows[0].items[0].null_flag_value == struct_pb2.NULL_VALUE, str(
            data.result.result_set
        )
        assert data.result.result_set.rows[0].items[1].null_flag_value == struct_pb2.NULL_VALUE, str(
            data.result.result_set
        )
        assert data.result.result_set.rows[0].items[2].null_flag_value == struct_pb2.NULL_VALUE, str(
            data.result.result_set
        )

    @yq_all
    @pytest.mark.parametrize(
        "filename", [("date_null/as_default/multi_null.csv"), ("date_null/parse_error/multi_null.csv")]
    )
    def test_string_not_null_multi(self, kikimr, s3, client, filename, unique_prefix):
        self.create_bucket_and_upload_file(filename, s3, kikimr)
        storage_connection_name = unique_prefix + "hcpp"
        client.create_storage_connection(storage_connection_name, "fbucket")

        sql = f'''
            SELECT
                `put`, `a`, `t`
            FROM
                `{storage_connection_name}`.`/{filename}`
            WITH (FORMAT="csv_with_names",
                csv_delimiter=",",
                SCHEMA=(
                `put` String NOT NULL,
                `a` Utf8 NOT NULL,
                `t` String NOT NULL
                ))
            LIMIT 10;
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
        data = client.get_result_data(query_id, limit=50)
        assert data.result.result_set.rows[0].items[0].bytes_value == b"", str(data.result.result_set)
        assert data.result.result_set.rows[0].items[1].bytes_value == b"", str(data.result.result_set)
        assert data.result.result_set.rows[0].items[2].bytes_value == b"", str(data.result.result_set)

    @yq_all
    def test_parquet_converters_to_timestamp(self, kikimr, s3, client, unique_prefix):
        # timestamp[ms] -> Timestamp
        # 2024-04-02T12:01:00.000Z
        data = [['apple'], [1712059260000]]

        # Define the schema for the data
        schema = pa.schema([('fruit', pa.string()), ('ts', pa.timestamp('ms'))])

        table = pa.Table.from_arrays(data, schema=schema)
        filename = 'test_parquet_converters_to_timestamp.parquet'
        pq.write_table(table, yatest_common.work_path(filename))
        s3_helpers.create_bucket_and_upload_file(filename, s3.s3_url, "fbucket", yatest_common.work_path())

        kikimr.control_plane.wait_bootstrap(1)
        storage_connection_name = unique_prefix + "hcpp"
        client.create_storage_connection(storage_connection_name, "fbucket")

        sql = f'''
            SELECT
                `fruit`, CAST(`ts` as String)
            FROM
                `{storage_connection_name}`.`/{filename}`
            WITH (FORMAT="parquet",
                SCHEMA=(
                `fruit` Utf8 NOT NULL,
                `ts` Timestamp
                ));
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
        data = client.get_result_data(query_id, limit=50)
        rows = data.result.result_set.rows
        assert len(rows) == 1, "invalid count rows"
        assert rows[0].items[0].text_value == "apple"
        assert rows[0].items[1].bytes_value == b"2024-04-02T12:01:00Z"

        # timestamp[us] -> Timestamp

        # 2024-04-02T12:01:00.000Z
        data = [['apple'], [1712059260000000]]

        # Define the schema for the data
        schema = pa.schema([('fruit', pa.string()), ('ts', pa.timestamp('us'))])

        table = pa.Table.from_arrays(data, schema=schema)
        pq.write_table(table, yatest_common.work_path(filename))
        s3_helpers.create_bucket_and_upload_file(filename, s3.s3_url, "fbucket", yatest_common.work_path())

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
        data = client.get_result_data(query_id, limit=50)
        rows = data.result.result_set.rows
        assert len(rows) == 1, "invalid count rows"
        assert rows[0].items[0].text_value == "apple"
        assert rows[0].items[1].bytes_value == b"2024-04-02T12:01:00Z"

        # string -> Timestamp

        # 2024-04-02T12:01:00.000Z
        data = [['apple'], ['2024-04-02 12:01:00']]

        # Define the schema for the data
        schema = pa.schema([('fruit', pa.string()), ('ts', pa.string())])

        table = pa.Table.from_arrays(data, schema=schema)
        pq.write_table(table, yatest_common.work_path(filename))
        s3_helpers.create_bucket_and_upload_file(filename, s3.s3_url, "fbucket", yatest_common.work_path())

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
        data = client.get_result_data(query_id, limit=50)
        rows = data.result.result_set.rows
        assert len(rows) == 1, "invalid count rows"
        assert rows[0].items[0].text_value == "apple"
        assert rows[0].items[1].bytes_value == b"2024-04-02T12:01:00Z"

        # utf8 -> Timestamp

        # 2024-04-02T12:01:00.000Z
        data = [['apple'], ['2024-04-02 12:01:00']]

        # Define the schema for the data
        schema = pa.schema([('fruit', pa.string()), ('ts', pa.utf8())])

        table = pa.Table.from_arrays(data, schema=schema)
        pq.write_table(table, yatest_common.work_path(filename))
        s3_helpers.create_bucket_and_upload_file(filename, s3.s3_url, "fbucket", yatest_common.work_path())

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
        data = client.get_result_data(query_id, limit=50)
        rows = data.result.result_set.rows
        assert len(rows) == 1, "invalid count rows"
        assert rows[0].items[0].text_value == "apple"
        assert rows[0].items[1].bytes_value == b"2024-04-02T12:01:00Z"

        # timestamp[s] -> Timestamp

        # 2024-04-02T12:01:00.000Z
        data = [['apple'], [1712059260]]

        # Define the schema for the data
        schema = pa.schema([('fruit', pa.string()), ('ts', pa.timestamp('s'))])

        table = pa.Table.from_arrays(data, schema=schema)
        pq.write_table(table, yatest_common.work_path(filename))
        s3_helpers.create_bucket_and_upload_file(filename, s3.s3_url, "fbucket", yatest_common.work_path())

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
        data = client.get_result_data(query_id, limit=50)
        rows = data.result.result_set.rows
        assert len(rows) == 1, "invalid count rows"
        assert rows[0].items[0].text_value == "apple"
        assert rows[0].items[1].bytes_value == b"2024-04-02T12:01:00Z"

        # timestamp[ns] -> Timestamp

        # 2024-04-02T12:01:00.000Z
        data = [['apple'], [1712059260000000000]]

        # Define the schema for the data
        schema = pa.schema([('fruit', pa.string()), ('ts', pa.timestamp('ns'))])

        table = pa.Table.from_arrays(data, schema=schema)
        pq.write_table(table, yatest_common.work_path(filename))
        s3_helpers.create_bucket_and_upload_file(filename, s3.s3_url, "fbucket", yatest_common.work_path())

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
        data = client.get_result_data(query_id, limit=50)
        rows = data.result.result_set.rows
        assert len(rows) == 1, "invalid count rows"
        assert rows[0].items[0].text_value == "apple"
        assert rows[0].items[1].bytes_value == b"2024-04-02T12:01:00Z"

        # date64 -> Timestamp

        # 2024-04-02T00:00:00.000Z
        data = [['apple'], [1712016000000]]

        # Define the schema for the data
        schema = pa.schema([('fruit', pa.string()), ('ts', pa.date64())])

        table = pa.Table.from_arrays(data, schema=schema)
        pq.write_table(table, yatest_common.work_path(filename))
        s3_helpers.create_bucket_and_upload_file(filename, s3.s3_url, "fbucket", yatest_common.work_path())

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
        data = client.get_result_data(query_id, limit=50)
        rows = data.result.result_set.rows
        assert len(rows) == 1, "invalid count rows"
        assert rows[0].items[0].text_value == "apple"
        assert rows[0].items[1].bytes_value == b"2024-04-02T00:00:00Z"

        # date32 -> Timestamp

        # 2024-04-02T00:00:00.000Z
        data = [['apple'], [19815]]

        # Define the schema for the data
        schema = pa.schema([('fruit', pa.string()), ('ts', pa.date32())])

        table = pa.Table.from_arrays(data, schema=schema)
        pq.write_table(table, yatest_common.work_path(filename))
        s3_helpers.create_bucket_and_upload_file(filename, s3.s3_url, "fbucket", yatest_common.work_path())

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
        data = client.get_result_data(query_id, limit=50)
        rows = data.result.result_set.rows
        assert len(rows) == 1, "invalid count rows"
        assert rows[0].items[0].text_value == "apple"
        assert rows[0].items[1].bytes_value == b"2024-04-02T00:00:00Z"

        # int32 [UNIX_TIME_SECONDS] -> Timestamp

        # 2024-04-02T00:00:00.000Z
        data = [['apple'], [1712059260]]

        # Define the schema for the data
        schema = pa.schema([('fruit', pa.string()), ('ts', pa.int32())])

        table = pa.Table.from_arrays(data, schema=schema)
        pq.write_table(table, yatest_common.work_path(filename))
        s3_helpers.create_bucket_and_upload_file(filename, s3.s3_url, "fbucket", yatest_common.work_path())

        sql = f'''
            SELECT
                `fruit`, CAST(`ts` as String)
            FROM
                `{storage_connection_name}`.`/{filename}`
            WITH (FORMAT="parquet",
                SCHEMA=(
                `fruit` Utf8 NOT NULL,
                `ts` Timestamp
                ),
                `data.timestamp.format_name`="UNIX_TIME_SECONDS");
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
        data = client.get_result_data(query_id, limit=50)
        rows = data.result.result_set.rows
        assert len(rows) == 1, "invalid count rows"
        assert rows[0].items[0].text_value == "apple"
        assert rows[0].items[1].bytes_value == b"2024-04-02T12:01:00Z"

        # int64 [UNIX_TIME_SECONDS] -> Timestamp

        # 2024-04-02T12:01:00.000Z
        data = [['apple'], [1712059260]]

        # Define the schema for the data
        schema = pa.schema([('fruit', pa.string()), ('ts', pa.int64())])

        table = pa.Table.from_arrays(data, schema=schema)
        pq.write_table(table, yatest_common.work_path(filename))
        s3_helpers.create_bucket_and_upload_file(filename, s3.s3_url, "fbucket", yatest_common.work_path())

        sql = f'''
            SELECT
                `fruit`, CAST(`ts` as String)
            FROM
                `{storage_connection_name}`.`/{filename}`
            WITH (FORMAT="parquet",
                SCHEMA=(
                `fruit` Utf8 NOT NULL,
                `ts` Timestamp
                ),
                `data.timestamp.format_name`="UNIX_TIME_SECONDS");
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
        data = client.get_result_data(query_id, limit=50)
        rows = data.result.result_set.rows
        assert len(rows) == 1, "invalid count rows"
        assert rows[0].items[0].text_value == "apple"
        assert rows[0].items[1].bytes_value == b"2024-04-02T12:01:00Z"

        # int64 [UNIX_TIME_MILLISECONDS] -> Timestamp

        # 2024-04-02T12:01:00.000Z
        data = [['apple'], [1712059260000]]

        # Define the schema for the data
        schema = pa.schema([('fruit', pa.string()), ('ts', pa.int64())])

        table = pa.Table.from_arrays(data, schema=schema)
        pq.write_table(table, yatest_common.work_path(filename))
        s3_helpers.create_bucket_and_upload_file(filename, s3.s3_url, "fbucket", yatest_common.work_path())

        sql = f'''
            SELECT
                `fruit`, CAST(`ts` as String)
            FROM
                `{storage_connection_name}`.`/{filename}`
            WITH (FORMAT="parquet",
                SCHEMA=(
                `fruit` Utf8 NOT NULL,
                `ts` Timestamp
                ),
                `data.timestamp.format_name`="UNIX_TIME_MILLISECONDS");
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
        data = client.get_result_data(query_id, limit=50)
        rows = data.result.result_set.rows
        assert len(rows) == 1, "invalid count rows"
        assert rows[0].items[0].text_value == "apple"
        assert rows[0].items[1].bytes_value == b"2024-04-02T12:01:00Z"

        # int64 [UNIX_TIME_MICROSECONDS] -> Timestamp

        # 2024-04-02T12:01:00.000Z
        data = [['apple'], [1712059260000000]]

        # Define the schema for the data
        schema = pa.schema([('fruit', pa.string()), ('ts', pa.int64())])

        table = pa.Table.from_arrays(data, schema=schema)
        pq.write_table(table, yatest_common.work_path(filename))
        s3_helpers.create_bucket_and_upload_file(filename, s3.s3_url, "fbucket", yatest_common.work_path())

        sql = f'''
            SELECT
                `fruit`, CAST(`ts` as String)
            FROM
                `{storage_connection_name}`.`/{filename}`
            WITH (FORMAT="parquet",
                SCHEMA=(
                `fruit` Utf8 NOT NULL,
                `ts` Timestamp
                ),
                `data.timestamp.format_name`="UNIX_TIME_MICROSECONDS");
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
        data = client.get_result_data(query_id, limit=50)
        rows = data.result.result_set.rows
        assert len(rows) == 1, "invalid count rows"
        assert rows[0].items[0].text_value == "apple"
        assert rows[0].items[1].bytes_value == b"2024-04-02T12:01:00Z"

        # uint32 [UNIX_TIME_SECONDS] -> Timestamp

        # 2024-04-02T00:00:00.000Z
        data = [['apple'], [1712059260]]

        # Define the schema for the data
        schema = pa.schema([('fruit', pa.string()), ('ts', pa.uint32())])

        table = pa.Table.from_arrays(data, schema=schema)
        pq.write_table(table, yatest_common.work_path(filename))
        s3_helpers.create_bucket_and_upload_file(filename, s3.s3_url, "fbucket", yatest_common.work_path())

        sql = f'''
            SELECT
                `fruit`, CAST(`ts` as String)
            FROM
                `{storage_connection_name}`.`/{filename}`
            WITH (FORMAT="parquet",
                SCHEMA=(
                `fruit` Utf8 NOT NULL,
                `ts` Timestamp
                ),
                `data.timestamp.format_name`="UNIX_TIME_SECONDS");
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
        data = client.get_result_data(query_id, limit=50)
        rows = data.result.result_set.rows
        assert len(rows) == 1, "invalid count rows"
        assert rows[0].items[0].text_value == "apple"
        assert rows[0].items[1].bytes_value == b"2024-04-02T12:01:00Z"

        # uint64 [UNIX_TIME_SECONDS] -> Timestamp

        # 2024-04-02T12:01:00.000Z
        data = [['apple'], [1712059260]]

        # Define the schema for the data
        schema = pa.schema([('fruit', pa.string()), ('ts', pa.uint64())])

        table = pa.Table.from_arrays(data, schema=schema)
        pq.write_table(table, yatest_common.work_path(filename))
        s3_helpers.create_bucket_and_upload_file(filename, s3.s3_url, "fbucket", yatest_common.work_path())

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
        data = client.get_result_data(query_id, limit=50)
        rows = data.result.result_set.rows
        assert len(rows) == 1, "invalid count rows"
        assert rows[0].items[0].text_value == "apple"
        assert rows[0].items[1].bytes_value == b"2024-04-02T12:01:00Z"

        # uint64 [UNIX_TIME_MILLISECONDS] -> Timestamp

        # 2024-04-02T12:01:00.000Z
        data = [['apple'], [1712059260000]]

        # Define the schema for the data
        schema = pa.schema([('fruit', pa.string()), ('ts', pa.uint64())])

        table = pa.Table.from_arrays(data, schema=schema)
        pq.write_table(table, yatest_common.work_path(filename))
        s3_helpers.create_bucket_and_upload_file(filename, s3.s3_url, "fbucket", yatest_common.work_path())

        sql = f'''
            SELECT
                `fruit`, CAST(`ts` as String)
            FROM
                `{storage_connection_name}`.`/{filename}`
            WITH (FORMAT="parquet",
                SCHEMA=(
                `fruit` Utf8 NOT NULL,
                `ts` Timestamp
                ),
                `data.timestamp.format_name`="UNIX_TIME_MILLISECONDS");
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
        data = client.get_result_data(query_id, limit=50)
        rows = data.result.result_set.rows
        assert len(rows) == 1, "invalid count rows"
        assert rows[0].items[0].text_value == "apple"
        assert rows[0].items[1].bytes_value == b"2024-04-02T12:01:00Z"

        # uint64 [UNIX_TIME_MICROSECONDS] -> Timestamp

        # 2024-04-02T12:01:00.000Z
        data = [['apple'], [1712059260000000]]

        # Define the schema for the data
        schema = pa.schema([('fruit', pa.string()), ('ts', pa.uint64())])

        table = pa.Table.from_arrays(data, schema=schema)
        pq.write_table(table, yatest_common.work_path(filename))
        s3_helpers.create_bucket_and_upload_file(filename, s3.s3_url, "fbucket", yatest_common.work_path())

        sql = f'''
            SELECT
                `fruit`, CAST(`ts` as String)
            FROM
                `{storage_connection_name}`.`/{filename}`
            WITH (FORMAT="parquet",
                SCHEMA=(
                `fruit` Utf8 NOT NULL,
                `ts` Timestamp
                ),
                `data.timestamp.format_name`="UNIX_TIME_MICROSECONDS");
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
        data = client.get_result_data(query_id, limit=50)
        rows = data.result.result_set.rows
        assert len(rows) == 1, "invalid count rows"
        assert rows[0].items[0].text_value == "apple"
        assert rows[0].items[1].bytes_value == b"2024-04-02T12:01:00Z"

        # uint16 [default] -> Timestamp

        # 2024-04-02T00:00:00.000Z
        data = [['apple'], [19815]]

        # Define the schema for the data
        schema = pa.schema([('fruit', pa.string()), ('ts', pa.uint16())])

        table = pa.Table.from_arrays(data, schema=schema)
        pq.write_table(table, yatest_common.work_path(filename))
        s3_helpers.create_bucket_and_upload_file(filename, s3.s3_url, "fbucket", yatest_common.work_path())

        sql = f'''
            SELECT
                `fruit`, CAST(`ts` as String)
            FROM
                `{storage_connection_name}`.`/{filename}`
            WITH (FORMAT="parquet",
                SCHEMA=(
                `fruit` Utf8 NOT NULL,
                `ts` Timestamp
                ));
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
        data = client.get_result_data(query_id, limit=50)
        rows = data.result.result_set.rows
        assert len(rows) == 1, "invalid count rows"
        assert rows[0].items[0].text_value == "apple"
        assert rows[0].items[1].bytes_value == b"2024-04-02T00:00:00Z"

    @yq_all
    def test_parquet_converters_to_datetime(self, kikimr, s3, client, unique_prefix):
        # timestamp[ms] -> Datetime
        # 2024-04-02T12:01:00.000Z
        data = [['apple'], [1712059260000]]

        # Define the schema for the data
        schema = pa.schema([('fruit', pa.string()), ('ts', pa.timestamp('ms'))])

        table = pa.Table.from_arrays(data, schema=schema)
        filename = 'test_parquet_converters_to_datetime.parquet'
        pq.write_table(table, yatest_common.work_path(filename))
        s3_helpers.create_bucket_and_upload_file(filename, s3.s3_url, "fbucket", yatest_common.work_path())

        kikimr.control_plane.wait_bootstrap(1)
        storage_connection_name = unique_prefix + "hcpp"
        client.create_storage_connection(storage_connection_name, "fbucket")

        sql = f'''
            SELECT
                `fruit`, CAST(`ts` as String)
            FROM
                `{storage_connection_name}`.`/{filename}`
            WITH (FORMAT="parquet",
                SCHEMA=(
                `fruit` Utf8 NOT NULL,
                `ts` Datetime
                ));
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.FAILED)
        describe_result = client.describe_query(query_id).result
        issues = describe_result.query.issue
        assert "millisecond accuracy does not fit into the datetime" in str(issues)

        # timestamp[us] -> Timestamp

        # 2024-04-02T12:01:00.000Z
        data = [['apple'], [1712059260000000]]

        # Define the schema for the data
        schema = pa.schema([('fruit', pa.string()), ('ts', pa.timestamp('us'))])

        table = pa.Table.from_arrays(data, schema=schema)
        pq.write_table(table, yatest_common.work_path(filename))
        s3_helpers.create_bucket_and_upload_file(filename, s3.s3_url, "fbucket", yatest_common.work_path())

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.FAILED)
        describe_result = client.describe_query(query_id).result
        issues = describe_result.query.issue
        assert "microsecond accuracy does not fit into the datetime" in str(issues)

        # timestamp[s] -> Timestamp

        # 2024-04-02T12:01:00.000Z
        data = [['apple'], [1712059260]]

        # Define the schema for the data
        schema = pa.schema([('fruit', pa.string()), ('ts', pa.timestamp('s'))])

        table = pa.Table.from_arrays(data, schema=schema)
        pq.write_table(table, yatest_common.work_path(filename))
        s3_helpers.create_bucket_and_upload_file(filename, s3.s3_url, "fbucket", yatest_common.work_path())

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.FAILED)
        describe_result = client.describe_query(query_id).result
        issues = describe_result.query.issue
        assert "millisecond accuracy does not fit into the datetime" in str(issues)

        # timestamp[ns] -> Timestamp

        # 2024-04-02T12:01:00.000Z
        data = [['apple'], [1712059260000000000]]

        # Define the schema for the data
        schema = pa.schema([('fruit', pa.string()), ('ts', pa.timestamp('ns'))])

        table = pa.Table.from_arrays(data, schema=schema)
        pq.write_table(table, yatest_common.work_path(filename))
        s3_helpers.create_bucket_and_upload_file(filename, s3.s3_url, "fbucket", yatest_common.work_path())

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.FAILED)
        describe_result = client.describe_query(query_id).result
        issues = describe_result.query.issue
        assert "microsecond accuracy does not fit into the datetime" in str(issues)

        # date64 -> Timestamp

        # 2024-04-02T00:00:00.000Z
        data = [['apple'], [1712016000000]]

        # Define the schema for the data
        schema = pa.schema([('fruit', pa.string()), ('ts', pa.date64())])

        table = pa.Table.from_arrays(data, schema=schema)
        pq.write_table(table, yatest_common.work_path(filename))
        s3_helpers.create_bucket_and_upload_file(filename, s3.s3_url, "fbucket", yatest_common.work_path())

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
        data = client.get_result_data(query_id, limit=50)
        rows = data.result.result_set.rows
        assert len(rows) == 1, "invalid count rows"
        assert rows[0].items[0].text_value == "apple"
        assert rows[0].items[1].bytes_value == b"2024-04-02T00:00:00Z"

        # date32 -> Timestamp

        # 2024-04-02T00:00:00.000Z
        data = [['apple'], [19815]]

        # Define the schema for the data
        schema = pa.schema([('fruit', pa.string()), ('ts', pa.date32())])

        table = pa.Table.from_arrays(data, schema=schema)
        pq.write_table(table, yatest_common.work_path(filename))
        s3_helpers.create_bucket_and_upload_file(filename, s3.s3_url, "fbucket", yatest_common.work_path())

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
        data = client.get_result_data(query_id, limit=50)
        assert len(rows) == 1, "invalid count rows"
        assert rows[0].items[0].text_value == "apple"
        assert rows[0].items[1].bytes_value == b"2024-04-02T00:00:00Z"

        # int32 -> Timestamp

        # 2024-04-02T00:00:00.000Z
        data = [['apple'], [1712059260]]

        # Define the schema for the data
        schema = pa.schema([('fruit', pa.string()), ('ts', pa.int32())])

        table = pa.Table.from_arrays(data, schema=schema)
        pq.write_table(table, yatest_common.work_path(filename))
        s3_helpers.create_bucket_and_upload_file(filename, s3.s3_url, "fbucket", yatest_common.work_path())

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
        data = client.get_result_data(query_id, limit=50)
        assert len(rows) == 1, "invalid count rows"
        assert rows[0].items[0].text_value == "apple"
        assert rows[0].items[1].bytes_value == b"2024-04-02T00:00:00Z"

        # int64 -> Timestamp

        # 2024-04-02T00:00:00.000Z
        data = [['apple'], [1712059260]]

        # Define the schema for the data
        schema = pa.schema([('fruit', pa.string()), ('ts', pa.int64())])

        table = pa.Table.from_arrays(data, schema=schema)
        pq.write_table(table, yatest_common.work_path(filename))
        s3_helpers.create_bucket_and_upload_file(filename, s3.s3_url, "fbucket", yatest_common.work_path())

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
        data = client.get_result_data(query_id, limit=50)
        assert len(data.result.result_set.rows) == 1, "invalid count rows"

        # uint32 -> Timestamp

        # 2024-04-02T00:00:00.000Z
        data = [['apple'], [1712059260]]

        # Define the schema for the data
        schema = pa.schema([('fruit', pa.string()), ('ts', pa.uint32())])

        table = pa.Table.from_arrays(data, schema=schema)
        pq.write_table(table, yatest_common.work_path(filename))
        s3_helpers.create_bucket_and_upload_file(filename, s3.s3_url, "fbucket", yatest_common.work_path())

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
        data = client.get_result_data(query_id, limit=50)
        assert len(rows) == 1, "invalid count rows"
        assert rows[0].items[0].text_value == "apple"
        assert rows[0].items[1].bytes_value == b"2024-04-02T00:00:00Z"

        # uint64 -> Timestamp

        # 2024-04-02T00:00:00.000Z
        data = [['apple'], [1712059260]]

        # Define the schema for the data
        schema = pa.schema([('fruit', pa.string()), ('ts', pa.uint64())])

        table = pa.Table.from_arrays(data, schema=schema)
        pq.write_table(table, yatest_common.work_path(filename))
        s3_helpers.create_bucket_and_upload_file(filename, s3.s3_url, "fbucket", yatest_common.work_path())

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
        data = client.get_result_data(query_id, limit=50)
        assert len(rows) == 1, "invalid count rows"
        assert rows[0].items[0].text_value == "apple"
        assert rows[0].items[1].bytes_value == b"2024-04-02T00:00:00Z"

        # uint16 [default] -> Timestamp

        # 2024-04-02T00:00:00.000Z
        data = [['apple'], [19815]]

        # Define the schema for the data
        schema = pa.schema([('fruit', pa.string()), ('ts', pa.uint16())])

        table = pa.Table.from_arrays(data, schema=schema)
        pq.write_table(table, yatest_common.work_path(filename))
        s3_helpers.create_bucket_and_upload_file(filename, s3.s3_url, "fbucket", yatest_common.work_path())

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
        data = client.get_result_data(query_id, limit=50)
        rows = data.result.result_set.rows
        assert len(rows) == 1, "invalid count rows"
        assert rows[0].items[0].text_value == "apple"
        assert rows[0].items[1].bytes_value == b"2024-04-02T00:00:00Z"

    @yq_all
    def test_parquet_converters_to_string(self, kikimr, s3, client, unique_prefix):
        # timestamp[ms] -> String
        # 2024-04-02T12:01:00.000000Z
        data = [['apple'], [1712059260000]]

        # Define the schema for the data
        schema = pa.schema([('fruit', pa.string()), ('ts', pa.timestamp('ms'))])

        table = pa.Table.from_arrays(data, schema=schema)
        filename = 'test_parquet_converters_to_string.parquet'
        pq.write_table(table, yatest_common.work_path(filename))
        s3_helpers.create_bucket_and_upload_file(filename, s3.s3_url, "fbucket", yatest_common.work_path())

        kikimr.control_plane.wait_bootstrap(1)
        storage_connection_name = unique_prefix + "hcpp"
        client.create_storage_connection(storage_connection_name, "fbucket")

        sql = f'''
            SELECT
                `fruit`, `ts`
            FROM
                `{storage_connection_name}`.`/{filename}`
            WITH (FORMAT="parquet",
                SCHEMA=(
                `fruit` Utf8 NOT NULL,
                `ts` String
                ));
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
        data = client.get_result_data(query_id, limit=50)
        rows = data.result.result_set.rows
        assert len(rows) == 1, "invalid count rows"
        assert rows[0].items[0].text_value == "apple"
        assert rows[0].items[1].bytes_value == b"2024-04-02T12:01:00.000000Z"

        # timestamp[us] -> String

        # 2024-04-02T12:01:00.000000Z
        data = [['apple'], [1712059260000000]]

        # Define the schema for the data
        schema = pa.schema([('fruit', pa.string()), ('ts', pa.timestamp('us'))])

        table = pa.Table.from_arrays(data, schema=schema)
        pq.write_table(table, yatest_common.work_path(filename))
        s3_helpers.create_bucket_and_upload_file(filename, s3.s3_url, "fbucket", yatest_common.work_path())

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
        data = client.get_result_data(query_id, limit=50)
        rows = data.result.result_set.rows
        assert len(rows) == 1, "invalid count rows"
        assert rows[0].items[0].text_value == "apple"
        assert rows[0].items[1].bytes_value == b"2024-04-02T12:01:00.000000Z"

        # timestamp[s] -> String

        # 2024-04-02T12:01:00.000000Z
        data = [['apple'], [1712059260]]

        # Define the schema for the data
        schema = pa.schema([('fruit', pa.string()), ('ts', pa.timestamp('s'))])

        table = pa.Table.from_arrays(data, schema=schema)
        pq.write_table(table, yatest_common.work_path(filename))
        s3_helpers.create_bucket_and_upload_file(filename, s3.s3_url, "fbucket", yatest_common.work_path())

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
        data = client.get_result_data(query_id, limit=50)
        rows = data.result.result_set.rows
        assert len(rows) == 1, "invalid count rows"
        assert rows[0].items[0].text_value == "apple"
        assert rows[0].items[1].bytes_value == b"2024-04-02T12:01:00.000000Z"

        # timestamp[ns] -> String

        # 2024-04-02T12:01:00.000000Z
        data = [['apple'], [1712059260000000000]]

        # Define the schema for the data
        schema = pa.schema([('fruit', pa.string()), ('ts', pa.timestamp('ns'))])

        table = pa.Table.from_arrays(data, schema=schema)
        pq.write_table(table, yatest_common.work_path(filename))
        s3_helpers.create_bucket_and_upload_file(filename, s3.s3_url, "fbucket", yatest_common.work_path())

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
        data = client.get_result_data(query_id, limit=50)
        rows = data.result.result_set.rows
        assert len(rows) == 1, "invalid count rows"
        assert rows[0].items[0].text_value == "apple"
        assert rows[0].items[1].bytes_value == b"2024-04-02T12:01:00.000000Z"

        # date64 -> String

        # 2024-04-02
        data = [['apple'], [1712016000000]]

        # Define the schema for the data
        schema = pa.schema([('fruit', pa.string()), ('ts', pa.date64())])

        table = pa.Table.from_arrays(data, schema=schema)
        pq.write_table(table, yatest_common.work_path(filename))
        s3_helpers.create_bucket_and_upload_file(filename, s3.s3_url, "fbucket", yatest_common.work_path())

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
        data = client.get_result_data(query_id, limit=50)
        rows = data.result.result_set.rows
        assert len(rows) == 1, "invalid count rows"
        assert rows[0].items[0].text_value == "apple"
        assert rows[0].items[1].bytes_value == b"2024-04-02"

        # date32 -> String

        # 2024-04-02
        data = [['apple'], [19815]]

        # Define the schema for the data
        schema = pa.schema([('fruit', pa.string()), ('ts', pa.date32())])

        table = pa.Table.from_arrays(data, schema=schema)
        pq.write_table(table, yatest_common.work_path(filename))
        s3_helpers.create_bucket_and_upload_file(filename, s3.s3_url, "fbucket", yatest_common.work_path())

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
        data = client.get_result_data(query_id, limit=50)
        rows = data.result.result_set.rows
        assert len(rows) == 1, "invalid count rows"
        assert rows[0].items[0].text_value == "apple"
        assert rows[0].items[1].bytes_value == b"2024-04-02"

    @yq_all
    def test_parquet_converters_to_utf8(self, kikimr, s3, client, unique_prefix):
        # timestamp[ms] -> Utf8
        # 2024-04-02T12:01:00.000000Z
        data = [['apple'], [1712059260000]]

        # Define the schema for the data
        schema = pa.schema([('fruit', pa.string()), ('ts', pa.timestamp('ms'))])

        table = pa.Table.from_arrays(data, schema=schema)
        filename = 'test_parquet_converters_to_utf8.parquet'
        pq.write_table(table, yatest_common.work_path(filename))
        s3_helpers.create_bucket_and_upload_file(filename, s3.s3_url, "fbucket", yatest_common.work_path())

        kikimr.control_plane.wait_bootstrap(1)
        storage_connection_name = unique_prefix + "hcpp"
        client.create_storage_connection(storage_connection_name, "fbucket")

        sql = f'''
            SELECT
                `fruit`, `ts`
            FROM
                `{storage_connection_name}`.`/{filename}`
            WITH (FORMAT="parquet",
                SCHEMA=(
                `fruit` Utf8 NOT NULL,
                `ts` Utf8
                ));
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
        data = client.get_result_data(query_id, limit=50)
        rows = data.result.result_set.rows
        assert len(rows) == 1, "invalid count rows"
        assert rows[0].items[0].text_value == "apple"
        assert rows[0].items[1].text_value == "2024-04-02T12:01:00.000000Z"

        # timestamp[us] -> Utf8

        # 2024-04-02T12:01:00.000000Z
        data = [['apple'], [1712059260000000]]

        # Define the schema for the data
        schema = pa.schema([('fruit', pa.string()), ('ts', pa.timestamp('us'))])

        table = pa.Table.from_arrays(data, schema=schema)
        pq.write_table(table, yatest_common.work_path(filename))
        s3_helpers.create_bucket_and_upload_file(filename, s3.s3_url, "fbucket", yatest_common.work_path())

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
        data = client.get_result_data(query_id, limit=50)
        rows = data.result.result_set.rows
        assert len(rows) == 1, "invalid count rows"
        assert rows[0].items[0].text_value == "apple"
        assert rows[0].items[1].text_value == "2024-04-02T12:01:00.000000Z"

        # timestamp[s] -> Utf8

        # 2024-04-02T12:01:00.000000Z
        data = [['apple'], [1712059260]]

        # Define the schema for the data
        schema = pa.schema([('fruit', pa.string()), ('ts', pa.timestamp('s'))])

        table = pa.Table.from_arrays(data, schema=schema)
        pq.write_table(table, yatest_common.work_path(filename))
        s3_helpers.create_bucket_and_upload_file(filename, s3.s3_url, "fbucket", yatest_common.work_path())

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
        data = client.get_result_data(query_id, limit=50)
        rows = data.result.result_set.rows
        assert len(rows) == 1, "invalid count rows"
        assert rows[0].items[0].text_value == "apple"
        assert rows[0].items[1].text_value == "2024-04-02T12:01:00.000000Z"

        # timestamp[ns] -> Utf8

        # 2024-04-02T12:01:00.000000Z
        data = [['apple'], [1712059260000000000]]

        # Define the schema for the data
        schema = pa.schema([('fruit', pa.string()), ('ts', pa.timestamp('ns'))])

        table = pa.Table.from_arrays(data, schema=schema)
        pq.write_table(table, yatest_common.work_path(filename))
        s3_helpers.create_bucket_and_upload_file(filename, s3.s3_url, "fbucket", yatest_common.work_path())

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
        data = client.get_result_data(query_id, limit=50)
        rows = data.result.result_set.rows
        assert len(rows) == 1, "invalid count rows"
        assert rows[0].items[0].text_value == "apple"
        assert rows[0].items[1].text_value == "2024-04-02T12:01:00.000000Z"

        # date64 -> Utf8

        # 2024-04-02
        data = [['apple'], [1712016000000]]

        # Define the schema for the data
        schema = pa.schema([('fruit', pa.string()), ('ts', pa.date64())])

        table = pa.Table.from_arrays(data, schema=schema)
        pq.write_table(table, yatest_common.work_path(filename))
        s3_helpers.create_bucket_and_upload_file(filename, s3.s3_url, "fbucket", yatest_common.work_path())

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
        data = client.get_result_data(query_id, limit=50)
        rows = data.result.result_set.rows
        assert len(rows) == 1, "invalid count rows"
        assert rows[0].items[0].text_value == "apple"
        assert rows[0].items[1].text_value == "2024-04-02"

        # date32 -> Utf8

        # 2024-04-02
        data = [['apple'], [19815]]

        # Define the schema for the data
        schema = pa.schema([('fruit', pa.string()), ('ts', pa.date32())])

        table = pa.Table.from_arrays(data, schema=schema)
        pq.write_table(table, yatest_common.work_path(filename))
        s3_helpers.create_bucket_and_upload_file(filename, s3.s3_url, "fbucket", yatest_common.work_path())

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
        data = client.get_result_data(query_id, limit=50)
        rows = data.result.result_set.rows
        assert len(rows) == 1, "invalid count rows"
        assert rows[0].items[0].text_value == "apple"
        assert rows[0].items[1].text_value == "2024-04-02"

    @yq_v2
    def test_s3_push_down_parquet(self, kikimr, s3, client, unique_prefix):
        data = [
            [
                int(datetime.fromisoformat("2024-06-17 00:00:00").timestamp() * 1000),
                int(datetime.fromisoformat("2024-06-16 00:00:00").timestamp() * 1000),
                int(datetime.fromisoformat("2024-06-15 00:00:00").timestamp() * 1000),
                int(datetime.fromisoformat("2024-06-14 00:00:00").timestamp() * 1000),
            ],
            ['apple', "banana", "pineapple", "watermelon"],
        ]

        # Define the schema for the data
        schema = pa.schema([('ts', pa.timestamp('ms')), ('fruit', pa.string())])

        table = pa.Table.from_arrays(data, schema=schema)
        filename = 'test_s3_push_down_parquet.parquet'
        pq.write_table(table, yatest_common.work_path(filename), row_group_size=2)
        s3_helpers.create_bucket_and_upload_file(filename, s3.s3_url, "fbucket", yatest_common.work_path())

        kikimr.control_plane.wait_bootstrap(1)
        storage_connection_name = unique_prefix + "hcpp"
        client.create_storage_connection(storage_connection_name, "fbucket")

        sql = f'''
            SELECT
                `fruit`, CAST(`ts` as Utf8)
            FROM
                `{storage_connection_name}`.`/{filename}`
            WITH (FORMAT="parquet",
                SCHEMA=(
                  `ts` Timestamp NOT NULL,
                  `fruit` Utf8 NOT NULL
                ))
            WHERE Timestamp("2024-06-14T00:00:00Z") <= ts and ts < Timestamp("2024-06-15T00:00:00Z")
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id

        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
        data = client.get_result_data(query_id, limit=50)
        rows = data.result.result_set.rows
        assert len(rows) == 1, "invalid count rows"
        assert rows[0].items[0].text_value == "watermelon"
        assert rows[0].items[1].text_value == "2024-06-14T00:00:00Z"

        stat = json.loads(client.describe_query(query_id).result.query.statistics.json)

        graph_name = "ResultSet"
        without_predicate_ingress_bytes = int(stat[graph_name]["IngressBytes"]["sum"])

        sql = 'pragma s3.UsePredicatePushdown = "true";\n' + sql
        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id

        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
        data = client.get_result_data(query_id, limit=50)
        rows = data.result.result_set.rows
        assert len(rows) == 1, "invalid count rows"
        assert rows[0].items[0].text_value == "watermelon"
        assert rows[0].items[1].text_value == "2024-06-14T00:00:00Z"

        stat = json.loads(client.describe_query(query_id).result.query.statistics.json)
        with_predicate_ingress_bytes = int(stat[graph_name]["IngressBytes"]["sum"])

        assert without_predicate_ingress_bytes > with_predicate_ingress_bytes, stat
