#!/usr/bin/env python
# -*- coding: utf-8 -*-

import boto3
import logging
import os
import pytest
import time
import ydb.public.api.protos.draft.fq_pb2 as fq
import ydb.public.api.protos.ydb_value_pb2 as ydb
from ydb.tests.library.common.helpers import plain_or_under_sanitizer
from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase
from ydb.tests.tools.fq_runner.kikimr_utils import yq_v1, yq_v2, yq_all
from google.protobuf.struct_pb2 import NullValue


class TestS3(TestYdsBase):
    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    @pytest.mark.parametrize("runtime_listing", ["false", "true"])
    def test_csv(self, kikimr, s3, client, runtime_listing, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("fbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

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
            pragma s3.UseRuntimeListing="{runtime_listing}";

            SELECT *
            FROM `{storage_connection_name}`.`fruits.csv`
            WITH (format=csv_with_names, SCHEMA (
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
        assert sum(kikimr.control_plane.get_metering(1)) == 10

    @yq_v2
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_inference(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("fbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        fruits = '''Fruit,Price,Weight,Date
Banana,3,100,2024-01-02
Apple,2,22,2024-03-04
Pear,15,33,2024-05-06'''
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='fruits.csv', ContentType='text/plain')
        kikimr.control_plane.wait_bootstrap(1)
        storage_connection_name = unique_prefix + "fruitbucket"
        client.create_storage_connection(storage_connection_name, "fbucket")

        sql = f'''
            SELECT *
            FROM `{storage_connection_name}`.`fruits.csv`
            WITH (format=csv_with_names, with_infer='true');
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        logging.debug(str(result_set))
        assert len(result_set.columns) == 4
        assert result_set.columns[0].name == "Date"
        assert result_set.columns[0].type.optional_type.item.type_id == ydb.Type.DATE
        assert result_set.columns[1].name == "Fruit"
        assert result_set.columns[1].type.type_id == ydb.Type.UTF8
        assert result_set.columns[2].name == "Price"
        assert result_set.columns[2].type.optional_type.item.type_id == ydb.Type.INT64
        assert result_set.columns[3].name == "Weight"
        assert result_set.columns[3].type.optional_type.item.type_id == ydb.Type.INT64
        assert len(result_set.rows) == 3
        assert result_set.rows[0].items[0].uint32_value == 19724
        assert result_set.rows[0].items[1].text_value == "Banana"
        assert result_set.rows[0].items[2].int64_value == 3
        assert result_set.rows[0].items[3].int64_value == 100
        assert result_set.rows[1].items[0].uint32_value == 19786
        assert result_set.rows[1].items[1].text_value == "Apple"
        assert result_set.rows[1].items[2].int64_value == 2
        assert result_set.rows[1].items[3].int64_value == 22
        assert result_set.rows[2].items[0].uint32_value == 19849
        assert result_set.rows[2].items[1].text_value == "Pear"
        assert result_set.rows[2].items[2].int64_value == 15
        assert result_set.rows[2].items[3].int64_value == 33
        assert sum(kikimr.control_plane.get_metering(1)) == 10

    @yq_v2
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_inference_null_column(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("fbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        fruits = '''Fruit,Price,Missing column
Banana,3,
Apple,2,
Pear,15,'''
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='fruits.csv', ContentType='text/plain')
        kikimr.control_plane.wait_bootstrap(1)
        storage_connection_name = unique_prefix + "fruitbucket"
        client.create_storage_connection(storage_connection_name, "fbucket")

        sql = f'''
            SELECT *
            FROM `{storage_connection_name}`.`fruits.csv`
            WITH (format=csv_with_names, with_infer='true');
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        logging.debug(str(result_set))
        assert len(result_set.columns) == 3
        assert result_set.columns[0].name == "Fruit"
        assert result_set.columns[0].type.type_id == ydb.Type.UTF8
        assert result_set.columns[1].name == "Missing column"
        assert result_set.columns[1].type.type_id == ydb.Type.UTF8
        assert result_set.columns[2].name == "Price"
        assert result_set.columns[2].type.optional_type.item.type_id == ydb.Type.INT64
        assert len(result_set.rows) == 3
        assert result_set.rows[0].items[0].text_value == "Banana"
        assert result_set.rows[0].items[1].text_value == ""
        assert result_set.rows[0].items[2].int64_value == 3
        assert result_set.rows[1].items[0].text_value == "Apple"
        assert result_set.rows[1].items[1].text_value == ""
        assert result_set.rows[1].items[2].int64_value == 2
        assert result_set.rows[2].items[0].text_value == "Pear"
        assert result_set.rows[2].items[1].text_value == ""
        assert result_set.rows[2].items[2].int64_value == 15
        assert sum(kikimr.control_plane.get_metering(1)) == 10

    @yq_v2
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_inference_optional_types(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("fbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        fruits = '''Fruit,Price,Weight,Date
Banana,,,2024-01-02
Apple,2,22,
,15,33,2024-05-06'''
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='fruits.csv', ContentType='text/plain')
        kikimr.control_plane.wait_bootstrap(1)
        storage_connection_name = unique_prefix + "fruitbucket"
        client.create_storage_connection(storage_connection_name, "fbucket")

        sql = f'''
            SELECT *
            FROM `{storage_connection_name}`.`fruits.csv`
            WITH (format=csv_with_names, with_infer='true');
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        logging.debug(str(result_set))
        assert len(result_set.columns) == 4
        assert result_set.columns[0].name == "Date"
        assert result_set.columns[0].type.optional_type.item.type_id == ydb.Type.DATE
        assert result_set.columns[1].name == "Fruit"
        assert result_set.columns[1].type.type_id == ydb.Type.UTF8
        assert result_set.columns[2].name == "Price"
        assert result_set.columns[2].type.optional_type.item.type_id == ydb.Type.INT64
        assert result_set.columns[3].name == "Weight"
        assert result_set.columns[3].type.optional_type.item.type_id == ydb.Type.INT64
        assert len(result_set.rows) == 3
        assert result_set.rows[0].items[0].uint32_value == 19724
        assert result_set.rows[0].items[1].text_value == "Banana"
        assert result_set.rows[0].items[2].null_flag_value == NullValue.NULL_VALUE
        assert result_set.rows[0].items[3].null_flag_value == NullValue.NULL_VALUE
        assert result_set.rows[1].items[0].null_flag_value == NullValue.NULL_VALUE
        assert result_set.rows[1].items[1].text_value == "Apple"
        assert result_set.rows[1].items[2].int64_value == 2
        assert result_set.rows[1].items[3].int64_value == 22
        assert result_set.rows[2].items[0].uint32_value == 19849
        assert result_set.rows[2].items[1].text_value == ""
        assert result_set.rows[2].items[2].int64_value == 15
        assert result_set.rows[2].items[3].int64_value == 33

    @yq_v2
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_inference_multiple_files(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("fbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        read_data_1 = '''a,b,c
1,2,3
1,2,3'''
        read_data_2 = '''a,b,c
1,2,3'''

        s3_client.put_object(Body=read_data_1, Bucket='fbucket', Key='test/sub dir/1.csv', ContentType='text/plain')
        s3_client.put_object(Body=read_data_2, Bucket='fbucket', Key='test/sub dir/2.csv', ContentType='text/plain')
        kikimr.control_plane.wait_bootstrap(1)
        storage_connection_name = unique_prefix + "multiple_files_bucket"
        client.create_storage_connection(storage_connection_name, "fbucket")

        sql = f'''
            SELECT *
            FROM `{storage_connection_name}`.`/test/`
            WITH (format=csv_with_names, with_infer='true');
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        logging.debug(str(result_set))
        assert len(result_set.columns) == 3
        assert result_set.columns[0].name == "a"
        assert result_set.columns[0].type.optional_type.item.type_id == ydb.Type.INT64
        assert result_set.columns[1].name == "b"
        assert result_set.columns[1].type.optional_type.item.type_id == ydb.Type.INT64
        assert result_set.columns[2].name == "c"
        assert result_set.columns[2].type.optional_type.item.type_id == ydb.Type.INT64
        assert len(result_set.rows) == 3
        assert result_set.rows[0].items[0].int64_value == 1
        assert result_set.rows[0].items[1].int64_value == 2
        assert result_set.rows[0].items[2].int64_value == 3
        assert result_set.rows[1].items[0].int64_value == 1
        assert result_set.rows[1].items[1].int64_value == 2
        assert result_set.rows[1].items[2].int64_value == 3
        assert result_set.rows[2].items[0].int64_value == 1
        assert result_set.rows[2].items[1].int64_value == 2
        assert result_set.rows[2].items[2].int64_value == 3
        assert sum(kikimr.control_plane.get_metering(1)) == 10

    @yq_v2
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_inference_file_error(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("fbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        read_data = '''{"a" : [10, 20, 30]}'''
        s3_client.put_object(Body=read_data, Bucket='fbucket', Key='data.json', ContentType='text/plain')
        kikimr.control_plane.wait_bootstrap(1)
        storage_connection_name = unique_prefix + "json_bucket"
        client.create_storage_connection(storage_connection_name, "fbucket")

        sql = f'''
            SELECT *
            FROM `{storage_connection_name}`.`data.json`
            WITH (format=csv_with_names, with_infer='true');
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.FAILED)
        assert "couldn\\'t open csv/tsv file, check format and compression parameters:" in str(
            client.describe_query(query_id).result
        )

    @yq_v2
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_inference_sigsegv(self, kikimr, s3, client, unique_prefix):
        # YQ-4511
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("fbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        read_data = '{' + (','.join(map(lambda k: f'"{k}":{k}', range(1024)))) + ',"1023":2}'
        s3_client.put_object(Body=read_data, Bucket='fbucket', Key='data.json', ContentType='text/plain')
        kikimr.control_plane.wait_bootstrap(1)
        storage_connection_name = unique_prefix + "json_bucket"
        client.create_storage_connection(storage_connection_name, "fbucket")

        sql = f'''
            SELECT *
            FROM `{storage_connection_name}`.`data.json`
            WITH (format=json_each_row, with_infer='true');
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.FAILED)
        assert "couldn\\'t open json file, check format and compression parameters:" in str(
            client.describe_query(query_id).result
        )

    @yq_v2
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_inference_parameters(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("fbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        fruits = '''Fruit|Price|Weight|Date
Banana|3|100|2024-01-02
Apple|2|22|2024-03-04
Pear|15|33|2024-05-06'''
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='year=10/month=5/test1.csv', ContentType='text/plain')
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='year=10/month=5/test2.csv', ContentType='text/plain')
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='year=10/month=5/test3.csv', ContentType='text/plain')
        kikimr.control_plane.wait_bootstrap(1)
        storage_connection_name = unique_prefix + "fruitbucket"
        client.create_storage_connection(storage_connection_name, "fbucket")

        sql = f'''
            SELECT *
            FROM `{storage_connection_name}`.`/`
            WITH (format=csv_with_names,
                with_infer='true',
                partitioned_by=(`year`, `month`),
                file_pattern='test*',
                csv_delimiter='|')
            limit 3;
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        logging.debug(str(result_set))
        assert len(result_set.columns) == 6
        assert result_set.columns[0].name == "Date"
        assert result_set.columns[0].type.optional_type.item.type_id == ydb.Type.DATE
        assert result_set.columns[1].name == "Fruit"
        assert result_set.columns[1].type.type_id == ydb.Type.UTF8
        assert result_set.columns[2].name == "Price"
        assert result_set.columns[2].type.optional_type.item.type_id == ydb.Type.INT64
        assert result_set.columns[3].name == "Weight"
        assert result_set.columns[3].type.optional_type.item.type_id == ydb.Type.INT64
        assert result_set.columns[4].name == "month"
        assert result_set.columns[4].type.type_id == ydb.Type.INT64
        assert result_set.columns[5].name == "year"
        assert result_set.columns[5].type.type_id == ydb.Type.INT64
        assert len(result_set.rows) == 3
        assert result_set.rows[0].items[0].uint32_value == 19724
        assert result_set.rows[0].items[1].text_value == "Banana"
        assert result_set.rows[0].items[2].int64_value == 3
        assert result_set.rows[0].items[3].int64_value == 100
        assert result_set.rows[0].items[4].int64_value == 5
        assert result_set.rows[0].items[5].int64_value == 10
        assert result_set.rows[1].items[0].uint32_value == 19786
        assert result_set.rows[1].items[1].text_value == "Apple"
        assert result_set.rows[1].items[2].int64_value == 2
        assert result_set.rows[1].items[3].int64_value == 22
        assert result_set.rows[1].items[4].int64_value == 5
        assert result_set.rows[1].items[5].int64_value == 10
        assert result_set.rows[2].items[0].uint32_value == 19849
        assert result_set.rows[2].items[1].text_value == "Pear"
        assert result_set.rows[2].items[2].int64_value == 15
        assert result_set.rows[2].items[3].int64_value == 33
        assert result_set.rows[2].items[4].int64_value == 5
        assert result_set.rows[2].items[5].int64_value == 10
        assert sum(kikimr.control_plane.get_metering(1)) == 10

    @yq_v2
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_inference_timestamp(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("fbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        csv_data = '''a,b,c
2024-08-29,2024-08-29 10:20:30,2024-08-29T10:20:30.01
2024-08-29,2024-08-29 10:20:30,2024-08-29T10:20:30.01
2024-08-29,2024-08-29 10:20:30,2024-08-29T10:20:30.01'''
        json_data = '''{ "a" : "2024-08-29", "b" : "2024-08-29 10:20:30", "c" : "2024-08-29T10:20:30.01" }
{ "a" : "2024-08-29", "b" : "2024-08-29 10:20:30", "c" : "2024-08-29T10:20:30.01" }
{ "a" : "2024-08-29", "b" : "2024-08-29 10:20:30", "c" : "2024-08-29T10:20:30.01" }'''
        s3_client.put_object(Body=csv_data, Bucket='fbucket', Key='timestamp.csv', ContentType='text/plain')
        s3_client.put_object(Body=json_data, Bucket='fbucket', Key='timestamp.json', ContentType='text/plain')
        kikimr.control_plane.wait_bootstrap(1)
        storage_connection_name = unique_prefix + "fruitbucket"
        client.create_storage_connection(storage_connection_name, "fbucket")

        sql = f'''
            SELECT *
            FROM `{storage_connection_name}`.`timestamp.csv`
            WITH (format=csv_with_names, with_infer='true');
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        logging.debug(str(result_set))
        assert len(result_set.columns) == 3
        assert result_set.columns[0].name == "a"
        assert result_set.columns[0].type.optional_type.item.type_id == ydb.Type.DATE
        assert result_set.columns[1].name == "b"
        assert result_set.columns[1].type.optional_type.item.type_id == ydb.Type.TIMESTAMP
        assert result_set.columns[2].name == "c"
        assert result_set.columns[2].type.type_id == ydb.Type.UTF8

        sql = f'''
            SELECT *
            FROM `{storage_connection_name}`.`timestamp.json`
            WITH (format=json_each_row, with_infer='true');
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        logging.debug(str(result_set))
        assert len(result_set.columns) == 3
        assert result_set.columns[0].name == "a"
        assert result_set.columns[0].type.type_id == ydb.Type.UTF8
        assert result_set.columns[1].name == "b"
        assert result_set.columns[1].type.type_id == ydb.Type.UTF8
        assert result_set.columns[2].name == "c"
        assert result_set.columns[2].type.type_id == ydb.Type.UTF8

        sql = f'''
            SELECT *
            FROM `{storage_connection_name}`.`timestamp.csv`
            WITH (format=csv_with_names, with_infer='true', `data.timestamp.formatname`='ISO');
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.FAILED)
        assert "parameter is not supported with type inference" in str(
            client.describe_query(query_id).result
        )

    @yq_v2
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_inference_projection(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("fbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        fruits = '''Fruit,Price,Weight
Banana,3,100
Apple,2,22
Pear,15,33'''
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='year=2023/fruits.csv', ContentType='text/plain')

        kikimr.control_plane.wait_bootstrap(1)
        storage_connection_name = unique_prefix + "fruitbucket"
        client.create_storage_connection(storage_connection_name, "fbucket")

        sql = '''$projection = @@ {
                "projection.enabled" : "true",
                "storage.location.template" : "/${date}",
                "projection.date.type" : "date",
                "projection.date.min" : "2022-11-02",
                "projection.date.max" : "2024-12-02",
                "projection.date.interval" : "1",
                "projection.date.format" : "/year=%Y",
                "projection.date.unit" : "YEARS"
            } @@;''' + f'''

            SELECT *
            FROM `{storage_connection_name}`.`/`
            WITH (format=csv_with_names,
                with_infer='true',
                partitioned_by=(`date`),
                projection=$projection);
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        logging.debug(str(result_set))
        assert len(result_set.columns) == 4
        assert result_set.columns[0].name == "Fruit"
        assert result_set.columns[0].type.type_id == ydb.Type.UTF8
        assert result_set.columns[1].name == "Price"
        assert result_set.columns[1].type.optional_type.item.type_id == ydb.Type.INT64
        assert result_set.columns[2].name == "Weight"
        assert result_set.columns[2].type.optional_type.item.type_id == ydb.Type.INT64
        assert result_set.columns[3].name == "date"
        assert result_set.columns[3].type.type_id == ydb.Type.DATE
        assert len(result_set.rows) == 3
        assert result_set.rows[0].items[0].text_value == "Banana"
        assert result_set.rows[0].items[1].int64_value == 3
        assert result_set.rows[0].items[2].int64_value == 100
        assert result_set.rows[0].items[3].uint32_value == 19663
        assert result_set.rows[1].items[0].text_value == "Apple"
        assert result_set.rows[1].items[1].int64_value == 2
        assert result_set.rows[1].items[2].int64_value == 22
        assert result_set.rows[1].items[3].uint32_value == 19663
        assert result_set.rows[2].items[0].text_value == "Pear"
        assert result_set.rows[2].items[1].int64_value == 15
        assert result_set.rows[2].items[2].int64_value == 33
        assert result_set.rows[2].items[3].uint32_value == 19663
        assert sum(kikimr.control_plane.get_metering(1)) == 10

    @yq_v2
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_inference_null_column_name(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("fbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        fruits = ''',Fruit,Price
1,Banana,3
2,Apple,2
3,Pear,15'''
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='fruits.csv', ContentType='text/plain')
        kikimr.control_plane.wait_bootstrap(1)
        storage_connection_name = unique_prefix + "fruitbucket"
        client.create_storage_connection(storage_connection_name, "fbucket")

        sql = f'''
            SELECT *
            FROM `{storage_connection_name}`.`fruits.csv`
            WITH (format=csv_with_names, with_infer='true');
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        logging.debug(str(result_set))
        assert len(result_set.columns) == 2
        assert result_set.columns[0].name == "Fruit"
        assert result_set.columns[0].type.type_id == ydb.Type.UTF8
        assert result_set.columns[1].name == "Price"
        assert result_set.columns[1].type.optional_type.item.type_id == ydb.Type.INT64
        assert len(result_set.rows) == 3
        assert result_set.rows[0].items[0].text_value == "Banana"
        assert result_set.rows[0].items[1].int64_value == 3
        assert result_set.rows[1].items[0].text_value == "Apple"
        assert result_set.rows[1].items[1].int64_value == 2
        assert result_set.rows[2].items[0].text_value == "Pear"
        assert result_set.rows[2].items[1].int64_value == 15
        assert sum(kikimr.control_plane.get_metering(1)) == 10

    @yq_v2
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_inference_unsupported_types(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("fbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        fruits = '''{ "a" : [10, 20, 30] , "b" : { "key" : "value" }, "c" : 10 }
{ "a" : [10, 20, 30] , "b" : { "key" : "value" }, "c" : 20 }
{ "a" : [10, 20, 30] , "b" : { "key" : "value" }, "c" : 30 }'''
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='fruits.json', ContentType='text/plain')
        kikimr.control_plane.wait_bootstrap(1)
        storage_connection_name = unique_prefix + "fruitbucket"
        client.create_storage_connection(storage_connection_name, "fbucket")

        sql = f'''
            SELECT *
            FROM `{storage_connection_name}`.`fruits.json`
            WITH (format=json_each_row, with_infer='true');
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        logging.debug(str(result_set))
        assert len(result_set.columns) == 1
        assert result_set.columns[0].name == "c"
        assert result_set.columns[0].type.optional_type.item.type_id == ydb.Type.INT64
        assert len(result_set.rows) == 3
        assert result_set.rows[0].items[0].int64_value == 10
        assert result_set.rows[1].items[0].int64_value == 20
        assert result_set.rows[2].items[0].int64_value == 30
        assert sum(kikimr.control_plane.get_metering(1)) == 10

    @yq_v2
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_json_list_formats(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("fbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        fruits = '''[
    { "date" : "", "datetime" : "", "timestamp" : "", "interval" : "", "date32" : "", "datetime64" : "", "timestamp64" : "", "interval64" : "", "tzDate" : "", "tzDateTime" : "", "tzTimestamp" : "" },
    { "date" : "", "datetime" : "", "timestamp" : "", "interval" : "", "date32" : "", "datetime64" : "", "timestamp64" : "", "interval64" : "", "tzDate" : "", "tzDateTime" : "", "tzTimestamp" : "" },
    { "date" : "", "datetime" : "", "timestamp" : "", "interval" : "", "date32" : "", "datetime64" : "", "timestamp64" : "", "interval64" : "", "tzDate" : "", "tzDateTime" : "", "tzTimestamp" : "" }
]'''
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='timestamp.json', ContentType='text/plain')

        kikimr.control_plane.wait_bootstrap(1)
        storage_connection_name = unique_prefix + "fruitbucket"
        client.create_storage_connection(storage_connection_name, "fbucket")

        sql = f'''
            SELECT *
            FROM `{storage_connection_name}`.`/timestamp.json`
            WITH (
                format="json_list",
                schema=(
                    `date` date,
                    `datetime` datetime,
                    `timestamp` timestamp,
                    `interval` interval,
                    `date32` date32,
                    `datetime64` datetime64,
                    `timestamp64` timestamp64,
                    `interval64` interval64,
                    `tzDate` tzDate,
                    `tzDateTime` tzDateTime,
                    `tzTimestamp` tzTimestamp
                ));
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.FAILED)

        error_message = str(client.describe_query(query_id).result)
        assert "Date, Timestamp and Interval types are not allowed in json_list format" in error_message
        assert "Date" in error_message
        assert "Datetime" in error_message
        assert "Timestamp" in error_message
        assert "Interval" in error_message
        assert "Date32" in error_message
        assert "Datetime64" in error_message
        assert "Timestamp64" in error_message
        assert "Interval64" in error_message
        assert "TzDate" in error_message
        assert "TzDatetime" in error_message
        assert "TzTimestamp" in error_message

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_csv_with_hopping(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("fbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        fruits = '''Time,Fruit,Price
0,Banana,3
1,Apple,2
2,Pear,15'''
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='fruits.csv', ContentType='text/plain')

        kikimr.control_plane.wait_bootstrap(1)
        storage_connection_name = unique_prefix + "fruitbucket"
        client.create_storage_connection(storage_connection_name, "fbucket")

        sql = fR'''
            SELECT COUNT(*) as count,
            FROM `{storage_connection_name}`.`fruits.csv`
            WITH (format=csv_with_names, SCHEMA (
                Time UInt64 NOT NULL,
                Fruit String NOT NULL,
                Price Int NOT NULL
            ))
            GROUP BY HOP(CAST(Time AS Timestamp?), "PT1M", "PT1M", "PT1M")
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        logging.debug(str(result_set))
        assert len(result_set.columns) == 1
        assert len(result_set.rows) == 1
        assert result_set.rows[0].items[0].uint64_value == 3

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    @pytest.mark.parametrize("runtime_listing", ["false", "true"])
    def test_raw(self, kikimr, s3, client, runtime_listing, yq_version, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("rbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        s3_client.put_object(Body="text1", Bucket='rbucket', Key='file1.txt', ContentType='text/plain')
        s3_client.put_object(Body="text3", Bucket='rbucket', Key='file3.txt', ContentType='text/plain')
        s3_client.put_object(Body="text2", Bucket='rbucket', Key='file2.txt', ContentType='text/plain')

        kikimr.control_plane.wait_bootstrap(1)
        storage_connection_name = unique_prefix + "rawbucket"
        client.create_storage_connection(storage_connection_name, "rbucket")

        sql = f'''
            pragma s3.UseRuntimeListing="{runtime_listing}";

            SELECT Data
            FROM `{storage_connection_name}`.`*`
            WITH (format=raw, SCHEMA (
                Data String NOT NULL
            ))
            ORDER BY Data DESC
            '''

        if yq_version == "v1":
            sql = 'pragma dq.MaxTasksPerStage="10"; ' + sql
        else:
            sql = 'pragma ydb.MaxTasksPerStage="10"; ' + sql

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        logging.debug(str(result_set))
        assert len(result_set.columns) == 1
        assert result_set.columns[0].name == "Data"
        assert result_set.columns[0].type.type_id == ydb.Type.STRING
        assert len(result_set.rows) == 3
        assert result_set.rows[0].items[0].bytes_value == b"text3"
        assert result_set.rows[1].items[0].bytes_value == b"text2"
        assert result_set.rows[2].items[0].bytes_value == b"text1"
        assert sum(kikimr.control_plane.get_metering(1)) == 10

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    @pytest.mark.parametrize("kikimr_params", [{"raw": 3, "": 4}], indirect=True)
    @pytest.mark.parametrize("runtime_listing", ["false", "true"])
    def test_limit(self, kikimr, s3, client, runtime_listing, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("lbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        s3_client.put_object(Body="text1", Bucket='lbucket', Key='file1.txt', ContentType='text/plain')

        kikimr.control_plane.wait_bootstrap(1)
        storage_connection_name = unique_prefix + "limbucket"
        client.create_storage_connection(storage_connection_name, "lbucket")

        sql = f'''
            pragma s3.UseRuntimeListing="{runtime_listing}";

            SELECT Data
            FROM `{storage_connection_name}`.`*`
            WITH (format=raw, SCHEMA (
                Data String
            ))
            ORDER BY Data DESC
            '''

        query_id = client.create_query("simple", sql).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.FAILED)
        assert "Size of object file1.txt = 5 and exceeds limit = 3 specified for format raw" in str(
            client.describe_query(query_id).result
        )

        sql = f'''
            pragma s3.UseRuntimeListing="{runtime_listing}";

            SELECT *
            FROM `{storage_connection_name}`.`*`
            WITH (format=csv_with_names, SCHEMA (
                Fruit String
            ));
            '''

        query_id = client.create_query("simple", sql).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.FAILED)
        assert "Size of object file1.txt = 5 and exceeds limit = 4 specified for format csv_with_names" in str(
            client.describe_query(query_id).result
        )

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    @pytest.mark.parametrize("runtime_listing", ["false", "true"])
    def test_bad_format(self, kikimr, s3, client, runtime_listing, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("bbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        s3_client.put_object(Body="blah blah blah", Bucket='bbucket', Key='file1.txt', ContentType='text/plain')

        kikimr.control_plane.wait_bootstrap(1)
        storage_connection_name = unique_prefix + "badbucket"
        client.create_storage_connection(storage_connection_name, "bbucket")

        sql = f'''
            pragma s3.UseRuntimeListing="{runtime_listing}";

            select * from `{storage_connection_name}`.`*.*` with (format=json_list, schema (data string)) limit 1;
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.FAILED)

    @yq_v2
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_bad_request_on_invalid_parquet(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("bbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        s3_client.put_object(Body='not a parquet file', Bucket='bbucket', Key='file.txt', ContentType='text/plain')

        kikimr.control_plane.wait_bootstrap(1)
        storage_connection_name = unique_prefix + "badbucket"
        client.create_storage_connection(storage_connection_name, "bbucket")

        sql = f'''
            select * from `{storage_connection_name}`.`file.txt` with (format=parquet, schema (data string));
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.FAILED)

        error_message = str(client.describe_query(query_id).result)
        assert ("Query failed with code BAD_REQUEST" in error_message) and ("Parquet magic bytes not found in footer." in error_message)

    @yq_v2
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_bad_request_on_compression(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("bbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        s3_client.put_object(Body="invalid uncompressed json file", Bucket='bbucket', Key='file.json', ContentType='text/plain')

        kikimr.control_plane.wait_bootstrap(1)
        storage_connection_name = unique_prefix + "badbucket"
        client.create_storage_connection(storage_connection_name, "bbucket")

        sql = f'''
            select * from `{storage_connection_name}`.`file.json` with (
                format=csv_with_names,
                compression='gzip',
                schema (data timestamp)
            );
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.FAILED)
        assert "Query failed with code BAD_REQUEST" in str(
            client.describe_query(query_id).result
        )

    @yq_v1
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    @pytest.mark.parametrize("mvp_external_ydb_endpoint", [{"endpoint": os.getenv("YDB_ENDPOINT")}], indirect=True)
    def test_checkpoints_on_join_s3_with_yds(self, kikimr, s3, client, unique_prefix):
        # Prepare S3
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket_name = "join_s3_with_yds"
        bucket = resource.Bucket(bucket_name)
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        def put_kv(k, v):
            json = '{}"key": {}, "value": "{}"{}'.format("{", k, v, "}")
            s3_client.put_object(Body=json, Bucket=bucket_name, Key='a/b/c/{}.json'.format(k), ContentType='text/json')

        put_kv(1, "one")
        put_kv(2, "two")
        put_kv(3, "three")

        kikimr.control_plane.wait_bootstrap(1)
        storage_connection_name = unique_prefix + "s3_dict"
        client.create_storage_connection(storage_connection_name, bucket_name)

        # Prepare YDS
        self.init_topics("yds_dict")
        yds_connection_name = unique_prefix + "yds"
        client.create_yds_connection(name=yds_connection_name, database_id="FakeDatabaseId")

        # Run query
        sql = f'''
            PRAGMA dq.MaxTasksPerStage="2";

            $s3_dict_raw =
                SELECT cast(Data AS json) AS data
                FROM `{storage_connection_name}`.`*`
                WITH (format=raw, SCHEMA (
                    Data String NOT NULL
                ));

            $s3_dict =
                SELECT
                    cast(JSON_VALUE(data, '$.key') AS int64) AS key,
                    cast(JSON_VALUE(data, '$.value') AS String) AS value
                FROM $s3_dict_raw;

            $parsed_yson_topic =
                SELECT
                    Yson::LookupInt64(yson_data, "key") AS key,
                    Yson::LookupString(yson_data, "val") AS val
                FROM (
                    SELECT
                        Yson::Parse(Data) AS yson_data
                    FROM `{yds_connection_name}`.`{self.input_topic}` WITH SCHEMA (Data String NOT NULL));

            $joined_seq =
                SELECT
                    s3_dict.value AS num,
                    yds_seq.val AS word
                FROM $parsed_yson_topic AS yds_seq
                    INNER JOIN $s3_dict AS s3_dict
                        ON yds_seq.key = s3_dict.key;

            INSERT INTO `{yds_connection_name}`.`{self.output_topic}`
            SELECT
                Yson::SerializeText(Yson::From(TableRow()))
            FROM $joined_seq;
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.STREAMING).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.RUNNING)
        kikimr.control_plane.wait_zero_checkpoint(query_id)

        yds_data = [
            '{"key" = 1; "val" = "January";}',
            '{"key" = 2; "val" = "February";}',
            '{"key" = 3; "val" = "March";}',
            '{"key" = 1; "val" = "Monday";}',
            '{"key" = 2; "val" = "Tuesday";}',
            '{"key" = 3; "val" = "Wednesday";}',
            '{"key" = 1; "val" = "Gold";}',
            '{"key" = 2; "val" = "Silver";}',
            '{"key" = 3; "val" = "Bronze";}',
        ]
        self.write_stream(yds_data)

        expected = [
            '{"num" = "one"; "word" = "January"}',
            '{"num" = "two"; "word" = "February"}',
            '{"num" = "three"; "word" = "March"}',
            '{"num" = "one"; "word" = "Monday"}',
            '{"num" = "two"; "word" = "Tuesday"}',
            '{"num" = "three"; "word" = "Wednesday"}',
            '{"num" = "one"; "word" = "Gold"}',
            '{"num" = "two"; "word" = "Silver"}',
            '{"num" = "three"; "word" = "Bronze"}',
        ]
        assert self.read_stream(len(expected)) == expected

        # Check that checkpointing is finished
        def wait_checkpoints(require_query_is_on=False):
            deadline = time.time() + plain_or_under_sanitizer(300, 900)
            while True:
                completed = kikimr.control_plane.get_completed_checkpoints(query_id, require_query_is_on)
                if completed >= 3:
                    break
                assert time.time() < deadline, "Completed: {}".format(completed)
                time.sleep(plain_or_under_sanitizer(0.5, 2))

        logging.debug("Wait checkpoints")
        wait_checkpoints(True)
        logging.debug("Wait checkpoints success")

        kikimr.control_plane.kikimr_cluster.nodes[1].stop()
        kikimr.control_plane.kikimr_cluster.nodes[1].start()
        kikimr.control_plane.wait_bootstrap(1)

        logging.debug("Wait checkpoints after restore")
        wait_checkpoints(False)
        logging.debug("Wait checkpoints after restore success")

        client.abort_query(query_id)
        client.wait_query(query_id)

    @yq_v2
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_double_optional_types_validation(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("fbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

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
            WITH (format='csv_with_names', SCHEMA (
                Name Int32??,
            ));
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.FAILED)
        issues = str(client.describe_query(query_id).result.query.issue)

        assert "double optional types are not supported" in issues, "Incorrect issues: " + issues

        sql = f'''
            INSERT INTO `{storage_connection_name}`.`insert/`
            WITH
            (
                FORMAT="csv_with_names"
            )
            SELECT CAST(42 AS Int32??) as Weight;'''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.FAILED)
        issues = str(client.describe_query(query_id).result.query.issue)

        assert "double optional types are not supported" in issues, "Incorrect issues: " + issues

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_json_list_validation(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("fbucket")
        bucket.create(ACL='public-read-write')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )
        fruits = '''Fruit,Price,Weight
Banana,3,100
Apple,2,22
Pear,15,33'''
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='fruits.csv', ContentType='text/plain')

        storage_connection_name = unique_prefix + "ibucket"
        client.create_storage_connection(storage_connection_name, "fbucket")

        sql = f'''
            SELECT * FROM `{storage_connection_name}`.`fruits.csv`
            WITH (
                FORMAT='json_list',
                SCHEMA (
                    Value Dict<Int32, String>,
                )
            );
        '''
        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.FAILED)
        issues = str(client.describe_query(query_id).result.query.issue)
        assert "unsupported dict key type, it should be String or Utf8" in issues, "Incorrect issues: " + issues

        sql = f"INSERT INTO `{storage_connection_name}`.`/test/`\n" + '''
            WITH (FORMAT = 'json_list')
            SELECT * FROM AS_TABLE([
                <|foo:{123: "abc"}, bar:"xxx"u|>
            ]);
        '''
        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.FAILED)
        issues = str(client.describe_query(query_id).result.query.issue)
        assert "unsupported dict key type, it should be String or Utf8" in issues, "Incorrect Issues: " + issues

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_schema_validation(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("fbucket")
        bucket.create(ACL='public-read-write')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )
        fruits = '''Fruit,Price,Weight
Banana,3,100
Apple,2,22
Pear,15,33'''
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='fruits.csv', ContentType='text/plain')

        storage_connection_name = unique_prefix + "ibucket"
        client.create_storage_connection(storage_connection_name, "fbucket")

        sql = f'''
            SELECT * FROM `{storage_connection_name}`.`fruits.csv`
            WITH (
                FORMAT='csv_with_names',
                SCHEMA (
                    Value Date32,
                )
            );
        '''
        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.FAILED)
        issues = str(client.describe_query(query_id).result.query.issue)
        assert "big dates is not supported" in issues, "Incorrect issues: " + issues

        sql = f'''
            INSERT INTO `{storage_connection_name}`.`/test/`
            WITH (FORMAT = 'csv_with_names')
            SELECT * FROM AS_TABLE([
                <|foo:CAST(CurrentUtcDate() AS Date32), bar:"xxx"u|>
            ]);
        '''
        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.FAILED)
        issues = str(client.describe_query(query_id).result.query.issue)
        assert "big dates is not supported" in issues, "Incorrect Issues: " + issues
