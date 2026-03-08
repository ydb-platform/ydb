#!/usr/bin/env python
# -*- coding: utf-8 -*-

import boto3
import logging
import pytest
import ydb.public.api.protos.draft.fq_pb2 as fq
import ydb.public.api.protos.ydb_value_pb2 as ydb
from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase
from ydb.tests.tools.fq_runner.kikimr_utils import yq_all


class TestS3FileColumns(TestYdsBase):
    """
    Tests for the file_columns setting on csv_with_names format.

    file_columns provides a virtual header row (same CSV rules as the real header)
    so that CSV files without a header row can be read.  SCHEMA defines types;
    file_columns defines the physical order of columns in the file.
    """

    def _make_s3_client(self, s3):
        return boto3.client(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key",
        )

    def _create_bucket(self, s3):
        resource = boto3.resource(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key",
        )
        bucket = resource.Bucket("fbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()
        return bucket

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_csv_with_names_file_columns(self, kikimr, s3, client, unique_prefix):
        """Basic test: CSV without header, file_columns order matches SCHEMA order."""
        self._create_bucket(s3)
        s3_client = self._make_s3_client(s3)

        # No header row; columns are Fruit, Price, Weight in file order
        fruits = 'Banana,3,100\nApple,2,22\nPear,15,33'
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='fruits_no_header.csv', ContentType='text/plain')

        kikimr.control_plane.wait_bootstrap(1)
        storage_connection_name = unique_prefix + "fruitbucket"
        client.create_storage_connection(storage_connection_name, "fbucket")

        sql = f'''
            SELECT *
            FROM `{storage_connection_name}`.`fruits_no_header.csv`
            WITH (format=csv_with_names, file_columns="Fruit,Price,Weight", SCHEMA (
                Fruit String NOT NULL,
                Price Int NOT NULL,
                Weight Int NOT NULL
            ));
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        logging.debug(str(result_set))
        assert len(result_set.columns) == 3
        assert result_set.columns[0].name == "Fruit"
        assert result_set.columns[0].type.type_id == ydb.Type.STRING
        assert result_set.columns[1].name == "Price"
        assert result_set.columns[1].type.type_id == ydb.Type.INT32
        assert result_set.columns[2].name == "Weight"
        assert result_set.columns[2].type.type_id == ydb.Type.INT32
        assert len(result_set.rows) == 3
        assert result_set.rows[0].items[0].bytes_value == b"Banana"
        assert result_set.rows[0].items[1].int32_value == 3
        assert result_set.rows[0].items[2].int32_value == 100
        assert result_set.rows[1].items[0].bytes_value == b"Apple"
        assert result_set.rows[1].items[1].int32_value == 2
        assert result_set.rows[1].items[2].int32_value == 22
        assert result_set.rows[2].items[0].bytes_value == b"Pear"
        assert result_set.rows[2].items[1].int32_value == 15
        assert result_set.rows[2].items[2].int32_value == 33

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_csv_with_names_file_columns_reordered(self, kikimr, s3, client, unique_prefix):
        """
        file_columns order differs from SCHEMA order.

        File has columns in order: Price, Fruit, Weight.
        SCHEMA lists them as: Fruit, Price, Weight.
        Mapping must be by name, not by position in SCHEMA.
        """
        self._create_bucket(s3)
        s3_client = self._make_s3_client(s3)

        # No header row; physical file order is Price, Fruit, Weight
        fruits = '3,Banana,100\n2,Apple,22\n15,Pear,33'
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='fruits_reordered.csv', ContentType='text/plain')

        kikimr.control_plane.wait_bootstrap(1)
        storage_connection_name = unique_prefix + "fruitbucket"
        client.create_storage_connection(storage_connection_name, "fbucket")

        sql = f'''
            SELECT *
            FROM `{storage_connection_name}`.`fruits_reordered.csv`
            WITH (format=csv_with_names, file_columns="Price,Fruit,Weight", SCHEMA (
                Fruit String NOT NULL,
                Price Int NOT NULL,
                Weight Int NOT NULL
            ));
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        logging.debug(str(result_set))
        assert len(result_set.columns) == 3
        assert result_set.columns[0].name == "Fruit"
        assert result_set.columns[0].type.type_id == ydb.Type.STRING
        assert result_set.columns[1].name == "Price"
        assert result_set.columns[1].type.type_id == ydb.Type.INT32
        assert result_set.columns[2].name == "Weight"
        assert result_set.columns[2].type.type_id == ydb.Type.INT32
        assert len(result_set.rows) == 3
        # Verify by-name mapping: Fruit=Banana even though it was column 2 in the file
        assert result_set.rows[0].items[0].bytes_value == b"Banana"
        assert result_set.rows[0].items[1].int32_value == 3
        assert result_set.rows[0].items[2].int32_value == 100
        assert result_set.rows[1].items[0].bytes_value == b"Apple"
        assert result_set.rows[1].items[1].int32_value == 2
        assert result_set.rows[1].items[2].int32_value == 22
        assert result_set.rows[2].items[0].bytes_value == b"Pear"
        assert result_set.rows[2].items[1].int32_value == 15
        assert result_set.rows[2].items[2].int32_value == 33

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_csv_with_names_file_columns_with_delimiter(self, kikimr, s3, client, unique_prefix):
        """
        file_columns is itself parsed with the same CSV rules as the file.

        When csvdelimiter=";" is set, file_columns must also use ";" as delimiter.
        """
        self._create_bucket(s3)
        s3_client = self._make_s3_client(s3)

        # No header row; semicolon-delimited; columns in order: Fruit, Price, Weight
        fruits = 'Banana;3;100\nApple;2;22\nPear;15;33'
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='fruits_semi.csv', ContentType='text/plain')

        kikimr.control_plane.wait_bootstrap(1)
        storage_connection_name = unique_prefix + "fruitbucket"
        client.create_storage_connection(storage_connection_name, "fbucket")

        sql = f'''
            SELECT *
            FROM `{storage_connection_name}`.`fruits_semi.csv`
            WITH (format=csv_with_names, csvdelimiter=";", file_columns="Fruit;Price;Weight", SCHEMA (
                Fruit String NOT NULL,
                Price Int NOT NULL,
                Weight Int NOT NULL
            ));
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        logging.debug(str(result_set))
        assert len(result_set.columns) == 3
        assert result_set.columns[0].name == "Fruit"
        assert result_set.columns[0].type.type_id == ydb.Type.STRING
        assert result_set.columns[1].name == "Price"
        assert result_set.columns[1].type.type_id == ydb.Type.INT32
        assert result_set.columns[2].name == "Weight"
        assert result_set.columns[2].type.type_id == ydb.Type.INT32
        assert len(result_set.rows) == 3
        assert result_set.rows[0].items[0].bytes_value == b"Banana"
        assert result_set.rows[0].items[1].int32_value == 3
        assert result_set.rows[0].items[2].int32_value == 100
        assert result_set.rows[1].items[0].bytes_value == b"Apple"
        assert result_set.rows[1].items[1].int32_value == 2
        assert result_set.rows[1].items[2].int32_value == 22
        assert result_set.rows[2].items[0].bytes_value == b"Pear"
        assert result_set.rows[2].items[1].int32_value == 15
        assert result_set.rows[2].items[2].int32_value == 33

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_csv_with_names_file_columns_via_binding(self, kikimr, s3, client, unique_prefix):
        """file_columns can also be supplied through a storage binding."""
        self._create_bucket(s3)
        s3_client = self._make_s3_client(s3)

        # No header row; columns in order: Price, Fruit, Weight
        fruits = '3,Banana,100\n2,Apple,22\n15,Pear,33'
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='fruits_binding.csv', ContentType='text/plain')

        kikimr.control_plane.wait_bootstrap(1)
        connection_response = client.create_storage_connection(unique_prefix + "fruitbucket", "fbucket")

        fruitType = ydb.Column(name="Fruit", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.STRING))
        priceType = ydb.Column(name="Price", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT32))
        weightType = ydb.Column(name="Weight", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT32))
        storage_binding_name = unique_prefix + "my_binding"
        client.create_object_storage_binding(
            name=storage_binding_name,
            path="fruits_binding.csv",
            format="csv_with_names",
            connection_id=connection_response.result.connection_id,
            columns=[fruitType, priceType, weightType],
            format_setting={"file_columns": "Price,Fruit,Weight"},
        )

        sql = f'''
            SELECT *
            FROM bindings.`{storage_binding_name}`;
            '''

        query_id = client.create_query("simple", sql).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        logging.debug(str(result_set))
        assert len(result_set.columns) == 3
        assert result_set.columns[0].name == "Fruit"
        assert result_set.columns[0].type.type_id == ydb.Type.STRING
        assert result_set.columns[1].name == "Price"
        assert result_set.columns[1].type.type_id == ydb.Type.INT32
        assert result_set.columns[2].name == "Weight"
        assert result_set.columns[2].type.type_id == ydb.Type.INT32
        assert len(result_set.rows) == 3
        assert result_set.rows[0].items[0].bytes_value == b"Banana"
        assert result_set.rows[0].items[1].int32_value == 3
        assert result_set.rows[0].items[2].int32_value == 100
        assert result_set.rows[1].items[0].bytes_value == b"Apple"
        assert result_set.rows[1].items[1].int32_value == 2
        assert result_set.rows[1].items[2].int32_value == 22
        assert result_set.rows[2].items[0].bytes_value == b"Pear"
        assert result_set.rows[2].items[1].int32_value == 15
        assert result_set.rows[2].items[2].int32_value == 33
