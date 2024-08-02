#!/usr/bin/env python
# -*- coding: utf-8 -*-

import boto3
import logging
import pytest
import time
import uuid
import ydb.public.api.protos.draft.fq_pb2 as fq
import ydb.public.api.protos.ydb_value_pb2 as ydb
import ydb.tests.library.common.yatest_common as yatest_common
from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase
from ydb.tests.tools.fq_runner.kikimr_utils import yq_v1, yq_v2, yq_all
from google.protobuf.struct_pb2 import NullValue


class TestS3(TestYdsBase):
    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    @pytest.mark.parametrize("runtime_listing", ["false", "true"])
    def test_csv(self, kikimr, s3, client, runtime_listing, yq_version):

        resource = boto3.resource(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("fbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        fruits = R'''Fruit,Price,Weight
Banana,3,100
Apple,2,22
Pear,15,33'''
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='fruits.csv', ContentType='text/plain')
        kikimr.control_plane.wait_bootstrap(1)
        client.create_storage_connection("fruitbucket", "fbucket")

        sql = f'''
            pragma s3.UseRuntimeListing="{runtime_listing}";

            SELECT *
            FROM fruitbucket.`fruits.csv`
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
        assert sum(kikimr.control_plane.get_metering()) == 10

    @yq_v2
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_inference(self, kikimr, s3, client):
        unique_prefix = str(uuid.uuid4())
        resource = boto3.resource(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("fbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
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
        assert result_set.columns[1].type.optional_type.item.type_id == ydb.Type.UTF8
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
        assert result_set.columns[0].type.optional_type.item.type_id == ydb.Type.UTF8
        assert result_set.columns[1].name == "Missing column"
        assert result_set.columns[1].type.optional_type.item.type_id == ydb.Type.UTF8
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
        assert result_set.columns[1].type.optional_type.item.type_id == ydb.Type.UTF8
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
        assert result_set.rows[2].items[1].null_flag_value == NullValue.NULL_VALUE
        assert result_set.rows[2].items[2].int64_value == 15
        assert result_set.rows[2].items[3].int64_value == 33

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_csv_with_hopping(self, kikimr, s3, client):
        resource = boto3.resource(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("fbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        fruits = R'''Time,Fruit,Price
0,Banana,3
1,Apple,2
2,Pear,15'''
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='fruits.csv', ContentType='text/plain')
        kikimr.control_plane.wait_bootstrap(1)
        client.create_storage_connection("fruitbucket", "fbucket")

        sql = R'''
            SELECT COUNT(*) as count,
            FROM fruitbucket.`fruits.csv`
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
    def test_raw(self, kikimr, s3, client, runtime_listing, yq_version):

        resource = boto3.resource(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("rbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        s3_client.put_object(Body="text1", Bucket='rbucket', Key='file1.txt', ContentType='text/plain')
        s3_client.put_object(Body="text3", Bucket='rbucket', Key='file3.txt', ContentType='text/plain')
        s3_client.put_object(Body="text2", Bucket='rbucket', Key='file2.txt', ContentType='text/plain')

        kikimr.control_plane.wait_bootstrap(1)
        client.create_storage_connection("rawbucket", "rbucket")

        sql = f'''
            pragma s3.UseRuntimeListing="{runtime_listing}";

            SELECT Data
            FROM rawbucket.`*`
            WITH (format=raw, SCHEMA (
                Data String NOT NULL
            ))
            ORDER BY Data DESC
            '''

        # if yq_version == "v1":
        sql = 'pragma dq.MaxTasksPerStage="10"; ' + sql

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
        assert sum(kikimr.control_plane.get_metering()) == 10

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    @pytest.mark.parametrize("kikimr", [{"raw": 3, "": 4}], indirect=True)
    @pytest.mark.parametrize("runtime_listing", ["false", "true"])
    def test_limit(self, kikimr, s3, client, runtime_listing, yq_version):

        resource = boto3.resource(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("lbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        s3_client.put_object(Body="text1", Bucket='lbucket', Key='file1.txt', ContentType='text/plain')

        kikimr.control_plane.wait_bootstrap(1)
        client.create_storage_connection("limbucket", "lbucket")

        sql = f'''
            pragma s3.UseRuntimeListing="{runtime_listing}";

            SELECT Data
            FROM limbucket.`*`
            WITH (format=raw, SCHEMA (
                Data String
            ))
            ORDER BY Data DESC
            '''

        query_id = client.create_query("simple", sql).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.FAILED)
        assert "Size of object file1.txt = 5 and exceeds limit = 3 specified for format raw" in str(client.describe_query(query_id).result)

        sql = f'''
            pragma s3.UseRuntimeListing="{runtime_listing}";

            SELECT *
            FROM limbucket.`*`
            WITH (format=csv_with_names, SCHEMA (
                Fruit String
            ));
            '''

        query_id = client.create_query("simple", sql).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.FAILED)
        assert "Size of object file1.txt = 5 and exceeds limit = 4 specified for format csv_with_names" in str(client.describe_query(query_id).result)

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    @pytest.mark.parametrize("runtime_listing", ["false", "true"])
    def test_bad_format(self, kikimr, s3, client, runtime_listing, yq_version):

        resource = boto3.resource(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("bbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        s3_client.put_object(Body="blah blah blah", Bucket='bbucket', Key='file1.txt', ContentType='text/plain')

        kikimr.control_plane.wait_bootstrap(1)
        client.create_storage_connection("badbucket", "bbucket")

        sql = f'''
            pragma s3.UseRuntimeListing="{runtime_listing}";

            select * from badbucket.`*.*` with (format=json_list, schema (data string)) limit 1;
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.FAILED)

    @yq_v1
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_checkpoints_on_join_s3_with_yds(self, kikimr, s3, client):
        # Prepare S3
        resource = boto3.resource(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        s3_client = boto3.client(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
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
        client.create_storage_connection("s3_dict", bucket_name)

        # Prepare YDS
        self.init_topics("yds_dict")
        client.create_yds_connection(name="yds", database_id="FakeDatabaseId")

        # Run query
        sql = f'''
            PRAGMA dq.MaxTasksPerStage="2";

            $s3_dict_raw =
                SELECT cast(Data AS json) AS data
                FROM s3_dict.`*`
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
                    FROM yds.`{self.input_topic}` WITH SCHEMA (Data String NOT NULL));

            $joined_seq =
                SELECT
                    s3_dict.value AS num,
                    yds_seq.val AS word
                FROM $parsed_yson_topic AS yds_seq
                    INNER JOIN $s3_dict AS s3_dict
                        ON yds_seq.key = s3_dict.key;

            INSERT INTO yds.`{self.output_topic}`
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
            deadline = time.time() + yatest_common.plain_or_under_sanitizer(300, 900)
            while True:
                completed = kikimr.control_plane.get_completed_checkpoints(query_id, require_query_is_on)
                if completed >= 3:
                    break
                assert time.time() < deadline, "Completed: {}".format(completed)
                time.sleep(yatest_common.plain_or_under_sanitizer(0.5, 2))

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

    @yq_v1  # v2 compute with multiple nodes is not supported yet
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    @pytest.mark.parametrize("kikimr", [{"compute": 3}], indirect=True)
    def test_write_result(self, kikimr, s3, client, yq_version):
        resource = boto3.resource(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("wbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        for i in range(100):
            j = i % 10
            fruit = "Fruit,Price,Weight\n"
            for k in range(j):
                fruit += "A" + str(j) + ",1,1\n"
            s3_client.put_object(Body=fruit, Bucket='wbucket', Key='fruits' + str(j) + '.csv', ContentType='text/plain')
        kikimr.control_plane.wait_bootstrap(1)
        kikimr.compute_plane.wait_bootstrap()
        client.create_storage_connection("fruitbucket", "wbucket")

        time.sleep(10) # 2 x node info update period

        sql = f'''
            SELECT Fruit, sum(Price) as Price, sum(Weight) as Weight
            FROM fruitbucket.`fruits*`
            WITH (format=csv_with_names, SCHEMA (
                Fruit String NOT NULL,
                Price Int NOT NULL,
                Weight Int NOT NULL
            )) group by Fruit;
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
        describe_result = client.describe_query(query_id)
        assert describe_result.result.query.result_set_meta[0].rows_count == 9

        data = client.get_result_data(query_id, limit=1000)
        result_set = data.result.result_set
        logging.debug(str(result_set))
        assert len(result_set.columns) == 3
        assert result_set.columns[0].name == "Fruit"
        assert result_set.columns[0].type.type_id == ydb.Type.STRING
        assert result_set.columns[1].name == "Price"
        assert result_set.columns[1].type.type_id == ydb.Type.INT64
        assert result_set.columns[2].name == "Weight"
        assert result_set.columns[2].type.type_id == ydb.Type.INT64
        assert len(result_set.rows) == 9
        assert sum(kikimr.control_plane.get_metering()) == 10

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    @pytest.mark.parametrize("runtime_listing", ["false", "true"])
    def test_precompute(self, kikimr, s3, client, runtime_listing, yq_version):

        resource = boto3.resource(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("pbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        s3_client.put_object(Body="text1", Bucket='pbucket', Key='file1.txt', ContentType='text/plain')
        s3_client.put_object(Body="text3", Bucket='pbucket', Key='file3.txt', ContentType='text/plain')
        s3_client.put_object(Body="text2", Bucket='pbucket', Key='file2.txt', ContentType='text/plain')

        kikimr.control_plane.wait_bootstrap(1)
        client.create_storage_connection("prebucket", "pbucket")

        sql = f'''
            pragma s3.UseRuntimeListing="{runtime_listing}";

            select count(*) as Cnt from prebucket.`file1.txt` with (format=raw, schema(
                Data String NOT NULL
            ))
            union all
            select count(*) as Cnt from prebucket.`file2.txt` with (format=raw, schema(
                Data String NOT NULL
            ))
            union all
            select count(*) as Cnt from prebucket.`file3.txt` with (format=raw, schema(
                Data String NOT NULL
            ))
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        logging.debug(str(result_set))
        assert len(result_set.columns) == 1
        assert result_set.columns[0].name == "Cnt"
        assert result_set.columns[0].type.type_id == ydb.Type.UINT64
        assert len(result_set.rows) == 3
        assert result_set.rows[0].items[0].uint64_value == 1
        assert result_set.rows[1].items[0].uint64_value == 1
        assert result_set.rows[2].items[0].uint64_value == 1
        assert sum(kikimr.control_plane.get_metering()) == 10

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    @pytest.mark.parametrize("runtime_listing", ["false", "true"])
    def test_failed_precompute(self, kikimr, s3, client, runtime_listing, yq_version):

        resource = boto3.resource(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("fpbucket")
        bucket.create(ACL='public-read-write')
        bucket.objects.all().delete()

        client.create_storage_connection("fp", "fpbucket")

        sql = f'''
            pragma s3.UseRuntimeListing="{runtime_listing}";

            insert into fp.`path/` with (format=json_each_row)
            select * from AS_TABLE([<|foo:123, bar:"xxx"u|>,<|foo:456, bar:"yyy"u|>]);
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        sql = f'''
            pragma s3.UseRuntimeListing="{runtime_listing}";

            select count(*) from fp.`path/` with (format=json_each_row, schema(
                foo Int NOT NULL,
                bar String NOT NULL
            ))
            union all
            select count(*) from fp.`path/` with (format=json_each_row, schema(
                foo String NOT NULL,
                bar Int NOT NULL
            ))
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.FAILED)

        issues = str(client.describe_query(query_id).result.query.issue)
        assert "Error while reading file path" in issues, "Incorrect Issues: " + issues
        assert "Cannot parse input" in issues, "Incorrect Issues: " + issues
        assert "while reading the value of key bar" in issues, "Incorrect Issues: " + issues

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    @pytest.mark.parametrize("runtime_listing", ["false", "true"])
    def test_missed(self, kikimr, s3, client, runtime_listing, yq_version):

        resource = boto3.resource(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("fbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        fruits = R'''Fruit,Price,Weight
Banana,3,100
Apple,2,22
Pear,15,33'''
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='fruits.csv', ContentType='text/plain')
        kikimr.control_plane.wait_bootstrap(1)
        client.create_storage_connection("fruitbucket", "fbucket")

        sql = f'''
            pragma s3.UseRuntimeListing="{runtime_listing}";

            SELECT *
            FROM fruitbucket.`fruits.csv`
            WITH (format=csv_with_names, SCHEMA (
                Fruit String NOT NULL,
                Price Int NOT NULL,
                Weight Int NOT NULL,
                Intellect String NOT NULL
            ));
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.FAILED)
        assert "Column `Intellect` is marked as not null, but was not found in the csv file" in "{}".format(client.describe_query(query_id).result)

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    @pytest.mark.parametrize("runtime_listing", ["false", "true"])
    def test_simple_hits_47(self, kikimr, s3, client, runtime_listing, yq_version):

        resource = boto3.resource(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("fbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        fruits = R'''Fruit,Price,Weight
Banana,3,100
Apple,2,22
Pear,15,33'''
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='fruits.csv', ContentType='text/plain')
        kikimr.control_plane.wait_bootstrap(1)
        client.create_storage_connection("fruitbucket", "fbucket")

        sql = f'''
            pragma s3.UseRuntimeListing="{runtime_listing}";

            $data = SELECT *
            FROM fruitbucket.`fruits.csv`
            WITH (format=csv_with_names, SCHEMA (
                Fruit String NOT NULL,
                Price Int NOT NULL,
                Weight Int NOT NULL
            ));

            $a = select CurrentUtcDate() as _date, Just(1.0) as parsed_lag from $data;

            SELECT
                SUM(parsed_lag)
            FROM  $a
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        logging.debug(str(result_set))
        assert len(result_set.columns) == 1
        assert result_set.columns[0].name == "column0"
        assert result_set.columns[0].type.optional_type.item.type_id == ydb.Type.DOUBLE
        assert len(result_set.rows) == 1
        assert result_set.rows[0].items[0].double_value == 3
        assert sum(kikimr.control_plane.get_metering()) == 10

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    @pytest.mark.parametrize("raw", [True, False])
    @pytest.mark.parametrize("path_pattern", ["exact_file", "directory_scan"])
    @pytest.mark.parametrize("runtime_listing", ["false", "true"])
    def test_i18n_unpartitioned(self, kikimr, s3, client, raw, path_pattern, runtime_listing, yq_version):

        resource = boto3.resource(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("ibucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        i18n_directory = 'dataset/こんにちは/'
        i18n_name = i18n_directory + 'fruitand&+ %непечатное.csv'

        fruits = R'''Data
101
102
103'''
        s3_client.put_object(Body=fruits, Bucket='ibucket', Key=i18n_name, ContentType='text/plain')
        kikimr.control_plane.wait_bootstrap(1)
        client.create_storage_connection("i18nbucket", "ibucket")

        if path_pattern == "exact_file":
            path = i18n_name
        elif path_pattern == "directory_scan":
            path = i18n_directory
        else:
            raise ValueError(f"Unknown path_pattern {path_pattern}")

        format = "raw" if raw else "csv_with_names"
        sql = f'''
            pragma s3.UseRuntimeListing="{runtime_listing}";

            SELECT count(*) as cnt
            FROM i18nbucket.`{path}`
            WITH (format={format}, SCHEMA (
                Data String
            ));
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        logging.debug(str(result_set))
        assert len(result_set.columns) == 1
        assert result_set.columns[0].name == "cnt"
        assert result_set.columns[0].type.type_id == ydb.Type.UINT64
        assert len(result_set.rows) == 1
        assert result_set.rows[0].items[0].uint64_value == 1 if raw else 3
        assert sum(kikimr.control_plane.get_metering()) == 10

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    @pytest.mark.parametrize("raw", [False, True])
    @pytest.mark.parametrize("partitioning", ["hive", "projection"])
    @pytest.mark.parametrize("runtime_listing", ["false", "true"])
    def test_i18n_partitioning(self, kikimr, s3, client, raw, partitioning, yq_version, runtime_listing):

        resource = boto3.resource(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("ibucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        i18n_name = 'fruit and &{+}% непечатное.csv'

        fruits = R'''Data
    101
    102
    103'''
        s3_client.put_object(Body=fruits, Bucket='ibucket', Key=f"dataset/folder=%こん/{i18n_name}", ContentType='text/plain')
        s3_client.put_object(Body=fruits, Bucket='ibucket', Key=f"dataset/folder=に ちは/{i18n_name}", ContentType='text/plain')
        s3_client.put_object(Body=fruits, Bucket='ibucket', Key=f"dataset/folder=に/{i18n_name}", ContentType='text/plain')
        kikimr.control_plane.wait_bootstrap(1)
        client.create_storage_connection("i18nbucket", "ibucket")

        format = "raw" if raw else "csv_with_names"
        if partitioning == "projection":
            sql = f'''
                pragma s3.UseRuntimeListing="{runtime_listing}";
                ''' + R'''
                $projection = @@ {
                    "projection.enabled" : "true",
                    "storage.location.template" : "/folder=${folder}",
                    "projection.folder.type" : "enum",
                    "projection.folder.values" : "%こん,に ちは,に"
                } @@;''' + f'''
                SELECT count(*) as cnt
                FROM i18nbucket.`dataset`
                WITH (
                    format={format},
                    SCHEMA (
                        Data String,
                        folder String NOT NULL
                    ),
                    partitioned_by=(folder),
                    projection=$projection
                )
                WHERE folder = 'に ちは' or folder = '%こん';
                '''
        elif partitioning == "hive":
            sql = f'''
                pragma s3.UseRuntimeListing="{runtime_listing}";

                SELECT count(*) as cnt
                FROM i18nbucket.`dataset`
                WITH (
                    format={format},
                    SCHEMA (
                        Data String,
                        folder String NOT NULL
                    ),
                    partitioned_by=(folder)
                )
                WHERE folder = 'に ちは' or folder = '%こん';
                '''
        else:
            raise ValueError(f"Unknown partitioning {partitioning}")

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        logging.debug(str(result_set))
        assert len(result_set.columns) == 1
        assert result_set.columns[0].name == "cnt"
        assert result_set.columns[0].type.type_id == ydb.Type.UINT64
        assert len(result_set.rows) == 1
        assert result_set.rows[0].items[0].uint64_value == 2 if raw else 6
        assert sum(kikimr.control_plane.get_metering()) == 10

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    @pytest.mark.parametrize("runtime_listing", ["false", "true"])
    def test_huge_source(self, kikimr, s3, client, runtime_listing, yq_version):

        resource = boto3.resource(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("hbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()
        kikimr.control_plane.wait_bootstrap(1)
        client.create_storage_connection("hugebucket", "hbucket")

        long_literal = "*" * 1024
        sql = f'''
            pragma s3.UseRuntimeListing="{runtime_listing}";

            insert into hugebucket.`path/` with (format=csv_with_names)
            select * from AS_TABLE(ListReplicate(<|s:"{long_literal}"u|>, 1024 * 10));
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        sql = f'''
            pragma s3.UseRuntimeListing="{runtime_listing}";

            select count(*) from hugebucket.`path/` with (format=csv_with_names, schema(
                s String NOT NULL
            ))
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        logging.debug(str(result_set))
        assert len(result_set.columns) == 1
        assert len(result_set.rows) == 1
        assert result_set.rows[0].items[0].uint64_value == 1024 * 10
        # 1024 x 1024 x 10 = 10 MB of raw data + little overhead for header, eols etc
        assert sum(kikimr.control_plane.get_metering()) == 21
