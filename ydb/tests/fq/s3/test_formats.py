#!/usr/bin/env python
# -*- coding: utf-8 -*-

import boto3
import json
import logging

import pytest

import ydb.public.api.protos.ydb_value_pb2 as ydb
import ydb.public.api.protos.draft.fq_pb2 as fq

import ydb.tests.fq.s3.s3_helpers as s3_helpers
from ydb.tests.tools.fq_runner.kikimr_utils import yq_all, YQ_STATS_FULL


class TestS3Formats:
    def create_bucket_and_upload_file(self, filename, s3, kikimr):
        s3_helpers.create_bucket_and_upload_file(filename, s3.s3_url, "fbucket", "ydb/tests/fq/s3/test_format_data")
        kikimr.control_plane.wait_bootstrap(1)

    def validate_result(self, result_set):
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

    def validate_pg_result(self, result_set):
        logging.debug(str(result_set))
        assert len(result_set.columns) == 3
        assert result_set.columns[0].name == "Fruit"
        assert result_set.columns[0].type.pg_type.oid == 25
        assert result_set.columns[1].name == "Price"
        assert result_set.columns[1].type.pg_type.oid == 23
        assert result_set.columns[2].name == "Weight"
        assert result_set.columns[2].type.pg_type.oid == 20
        assert len(result_set.rows) == 3
        assert result_set.rows[0].items[0].text_value == "Banana"
        assert result_set.rows[0].items[1].text_value == "3"
        assert result_set.rows[0].items[2].text_value == "100"
        assert result_set.rows[1].items[0].text_value == "Apple"
        assert result_set.rows[1].items[1].text_value == "2"
        assert result_set.rows[1].items[2].text_value == "22"
        assert result_set.rows[2].items[0].text_value == "Pear"
        assert result_set.rows[2].items[1].text_value == "15"
        assert result_set.rows[2].items[2].text_value == "33"

    @yq_all
    @pytest.mark.parametrize("kikimr_settings", [{"stats_mode": YQ_STATS_FULL}], indirect=True)
    @pytest.mark.parametrize(
        "filename, type_format",
        [
            ("test.csv", "csv_with_names"),
            ("test.tsv", "tsv_with_names"),
            ("test.json", "json_each_row"),
            ("test.json", "json_list"),
            ("test.parquet", "parquet"),
        ],
    )
    def test_format(self, kikimr, s3, client, filename, type_format, yq_version, unique_prefix):
        self.create_bucket_and_upload_file(filename, s3, kikimr)
        storage_connection_name = unique_prefix + "fruitbucket"
        client.create_storage_connection(storage_connection_name, "fbucket")

        sql = f'''
            PRAGMA s3.UseBlocksSource="true";
            SELECT *
            FROM `{storage_connection_name}`.`{filename}`
            WITH (format=`{type_format}`, SCHEMA (
                Fruit String NOT NULL,
                Price Int NOT NULL,
                Weight Int NOT NULL
            ));
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        self.validate_result(result_set)

        stat = json.loads(client.describe_query(query_id).result.query.statistics.json)
        if yq_version == "v1":
            assert stat["Graph=0"]["TaskRunner"]["Stage=Total"]["IngressBytes"]["sum"] > 0
            if type_format != "json_list":
                assert stat["Graph=0"]["TaskRunner"]["Stage=Total"]["IngressRows"]["sum"] == 3
        else:  # v2
            assert stat["ResultSet"]["IngressBytes"]["sum"] > 0
            if type_format != "json_list":
                assert stat["ResultSet"]["IngressRows"]["sum"] == 3

    @yq_all
    def test_btc(self, kikimr, s3, client, unique_prefix):
        self.create_bucket_and_upload_file("btct.parquet", s3, kikimr)
        storage_connection_name = unique_prefix + "btct"
        client.create_storage_connection(storage_connection_name, "fbucket")

        sql = f'''
            PRAGMA s3.UseBlocksSource="true";
            SELECT
                *
            FROM `{storage_connection_name}`.`btct.parquet`
            WITH (format=`parquet`,
                SCHEMA=(
                    hash STRING,
                    version INT64,
                    size INT64,
                    block_hash UTF8,
                    block_number INT64,
                    virtual_size INT64,
                    lock_time INT64,
                    input_count INT64,
                    output_count INT64,
                    is_coinbase BOOL,
                    output_value DOUBLE,
                    block_timestamp DATETIME,
                    date DATE
                )
            );
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.FAILED)
        describe_result = client.describe_query(query_id).result
        issues = describe_result.query.issue[0].issues
        assert "Error while reading file btct.parquet" in issues[0].message
        assert "File contains LIST field outputs and can\'t be parsed" in issues[0].issues[0].message

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_invalid_format(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("fbucket")
        bucket.create(ACL='public-read')

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        fruits = '''Fruit,Price,Weight
Banana,3,100
Apple,2,22
Pear,15,33'''
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='fruits.csv', ContentType='text/plain')
        kikimr.control_plane.wait_bootstrap(1)

        storage_connection_name = unique_prefix + "fruitbucket"
        client.create_storage_connection(storage_connection_name, "fbucket")

        sql = f'''
            SELECT *
            FROM `{storage_connection_name}`.`fruits.csv`
            WITH (format=invalid_type_format, SCHEMA (
                Fruit String,
                Price Int,
                Weight Int
            ));
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.FAILED)
        describe_result = client.describe_query(query_id).result
        logging.debug("Describe result: {}".format(describe_result))
        describe_string = "{}".format(describe_result)
        assert (
            "Unknown format: invalid_type_format. Use one of: csv_with_names, tsv_with_names, json_list, json, raw, json_as_string, json_each_row, parquet"
            in describe_string
        )

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_invalid_input_compression(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("ibucket")
        bucket.create(ACL='public-read')

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        s3_client.put_object(Body="blahblahblah", Bucket='ibucket', Key='fruits', ContentType='text/plain')

        kikimr.control_plane.wait_bootstrap(1)
        storage_connection_name = unique_prefix + "input_bucket"
        client.create_storage_connection(storage_connection_name, "ibucket")

        sql = f'''
            SELECT *
            FROM `{storage_connection_name}`.`*`
            WITH (format=parquet, compression=abvgzip, SCHEMA (
                Fruit String,
                Price Int,
                Weight Int
            ));
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.FAILED)
        describe_result = client.describe_query(query_id).result
        assert "External compression for parquet is not supported" in str(describe_result)

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_invalid_output_compression(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("obucket")
        bucket.create(ACL='public-read')

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        s3_client.put_object(Body="blahblahblah", Bucket='obucket', Key='fruits', ContentType='text/plain')

        kikimr.control_plane.wait_bootstrap(1)
        storage_connection_name = unique_prefix + "output_bucket"
        client.create_storage_connection(storage_connection_name, "obucket")

        sql = f'''
            INSERT INTO `{storage_connection_name}`.`path/` with (format=parquet, compression=abvgzip)
            SELECT *
            FROM `{storage_connection_name}`.`*`
            WITH (format=parquet, SCHEMA (
                Fruit String,
                Price Int,
                Weight Int
            ));
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.FAILED)
        describe_result = client.describe_query(query_id).result
        assert "External compression for parquet is not supported" in str(describe_result)

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_custom_csv_delimiter_format(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("fbucket")
        bucket.create(ACL='public-read')

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        fruits = '''Fruit;Price;Weight
    Banana;3;100
    Apple;2;22
    Pear;15;33'''
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='fruits.csv', ContentType='text/plain')
        kikimr.control_plane.wait_bootstrap(1)

        storage_connection_name = unique_prefix + "fruitbucket"
        client.create_storage_connection(storage_connection_name, "fbucket")

        sql = f'''
                SELECT *
                FROM `{storage_connection_name}`.`fruits.csv`
                WITH (
                    format = csv_with_names,
                    csv_delimiter = ";",
                    schema = (
                        Fruit String NOT NULL,
                        Price Int NOT NULL,
                        Weight Int NOT NULL
                    )
                );
                '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        self.validate_result(result_set)

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_no_not_nullable_column(self, kikimr, s3, client, unique_prefix):
        filename = "test_wrong_type.csv"
        self.create_bucket_and_upload_file(filename, s3, kikimr)
        storage_connection_name = unique_prefix + "fruitbucket"
        client.create_storage_connection(storage_connection_name, "fbucket")

        sql = f'''
            SELECT *
            FROM `{storage_connection_name}`.`{filename}`
            WITH (format=`csv_with_names`, SCHEMA (
                Fruit String,
                AMOGUS String not null
            ));
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.FAILED)
        describe_result = client.describe_query(query_id).result
        logging.debug("Describe result: {}".format(describe_result))
        describe_string = "{}".format(describe_result)
        assert (
            "Column `AMOGUS` is marked as not null, but was not found in the csv file" in describe_string
        ), describe_string

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_no_nullable_column(self, kikimr, s3, client, unique_prefix):
        filename = "test_wrong_type.csv"
        self.create_bucket_and_upload_file(filename, s3, kikimr)
        storage_connection_name = unique_prefix + "fruitbucket"
        client.create_storage_connection(storage_connection_name, "fbucket")

        sql = f'''
            SELECT *
            FROM `{storage_connection_name}`.`{filename}`
            WITH (format=`csv_with_names`, SCHEMA (
                Fruit String,
                AMOGUS String
            ));
            '''

        query_id = client.create_query("simple", sql).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        assert result_set.rows[0].items[0].HasField("null_flag_value")

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_invalid_column_type_in_csv(self, kikimr, s3, client, unique_prefix):
        filename = "test_wrong_type.csv"
        self.create_bucket_and_upload_file(filename, s3, kikimr)
        storage_connection_name = unique_prefix + "fruitbucket"
        client.create_storage_connection(storage_connection_name, "fbucket")

        sql1 = f'''
            SELECT *
            FROM `{storage_connection_name}`.`{filename}`
            WITH (format=`csv_with_names`, SCHEMA (
                Fruit String,
                Owner Int
            ));
            '''

        sql2 = f'''
            SELECT *
            FROM `{storage_connection_name}`.`{filename}`
            WITH (format=`csv_with_names`, SCHEMA (
                Fruit Int,
                Owner String
            ));
            '''

        def check_result(query_id, corrupted_col_name):
            client.wait_query_status(query_id, fq.QueryMeta.FAILED)
            describe_result = client.describe_query(query_id).result
            logging.debug("Describe result: {}".format(describe_result))
            describe_string = "{}".format(describe_result)
            assert (
                "failed to parse data in column `{}\\' from row 0, probably data type differs from specified in schema".format(
                    corrupted_col_name
                )
                in describe_string
            ), describe_string

        query1_id = client.create_query("simple", sql1, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        query2_id = client.create_query("simple", sql2, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        check_result(query1_id, "Owner")
        check_result(query2_id, "Fruit")

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_invalid_column_in_parquet(self, kikimr, s3, client, unique_prefix):
        self.create_bucket_and_upload_file("test.parquet", s3, kikimr)
        storage_connection_name = unique_prefix + "fruitbucket"
        client.create_storage_connection(storage_connection_name, "fbucket")

        sql = f'''
            SELECT *
            FROM `{storage_connection_name}`.`test.parquet`
            WITH (format=parquet, SCHEMA (
                Fruit String,
                Price Int,
                ZZZZZ String
            ));
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.FAILED)
        describe_result = client.describe_query(query_id).result
        logging.debug("Describe result: {}".format(describe_result))
        issues = describe_result.query.issue[0].issues
        assert '''Error while reading file test.parquet''' in issues[0].message
        assert "Missing field: ZZZZZ" in issues[0].issues[0].message

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_simple_pg_types(self, kikimr, s3, client, unique_prefix):
        self.create_bucket_and_upload_file("test.parquet", s3, kikimr)
        storage_connection_name = unique_prefix + "fruitbucket"
        client.create_storage_connection(storage_connection_name, "fbucket")

        sql = f'''
            SELECT *
            FROM `{storage_connection_name}`.`test.parquet`
            WITH (format=parquet, SCHEMA (
                Fruit pgtext,
                Price pgint4,
                Weight pgint8
            ));
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        self.validate_pg_result(result_set)

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_precompute(self, kikimr, s3, client, unique_prefix):
        self.create_bucket_and_upload_file("test.parquet", s3, kikimr)
        storage_connection_name = unique_prefix + "fruitbucket"
        client.create_storage_connection(storage_connection_name, "fbucket")

        sql = f'''
            $maxPrice = SELECT MAX(Price)
            FROM `{storage_connection_name}`.`test.parquet`
            WITH (format=`parquet`, SCHEMA (
                Fruit String NOT NULL,
                Price Int NOT NULL,
                Weight Int NOT NULL
            ));

            SELECT Fruit, Price
            FROM `{storage_connection_name}`.`test.parquet`
            WITH (format=`parquet`, SCHEMA (
                Fruit String NOT NULL,
                Price Int NOT NULL,
                Weight Int NOT NULL
            ))
            WHERE Price == $maxPrice;
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        logging.debug(str(result_set))
        assert len(result_set.columns) == 2
        assert result_set.columns[0].name == "Fruit"
        assert result_set.columns[0].type.type_id == ydb.Type.STRING
        assert result_set.columns[1].name == "Price"
        assert result_set.columns[1].type.type_id == ydb.Type.INT32
        assert len(result_set.rows) == 1
        assert result_set.rows[0].items[0].bytes_value == b"Pear"
        assert result_set.rows[0].items[1].int32_value == 15

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_raw_empty_schema_query(self, kikimr, s3, client, unique_prefix):
        self.create_bucket_and_upload_file("test.parquet", s3, kikimr)
        storage_connection_name = unique_prefix + "fruitbucket"
        client.create_storage_connection(storage_connection_name, "fbucket")
        sql = f'''
            SELECT * FROM `{storage_connection_name}`.`*`
            WITH (format=raw, SCHEMA ());
            '''

        query_id = client.create_query(
            "test_raw_empty_schema", sql, type=fq.QueryContent.QueryType.ANALYTICS
        ).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.FAILED)
        describe_result = client.describe_query(query_id).result
        describe_string = "{}".format(describe_result)
        assert r"Only one column in schema supported in raw format" in describe_string
