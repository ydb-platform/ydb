#!/usr/bin/env python
# -*- coding: utf-8 -*-

import boto3
import json

import pytest

import ydb.public.api.protos.draft.fq_pb2 as fq
from ydb.tests.tools.fq_runner.kikimr_utils import yq_all


class TestS3(object):

    @yq_all
    @pytest.mark.parametrize("format", ["json_list", "json_each_row", "csv_with_names", "parquet"])
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_egress(self, kikimr, s3, client, format, yq_version, unique_prefix):
        resource = boto3.resource(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("egress_bucket")
        bucket.create(ACL='public-read-write')

        kikimr.control_plane.wait_bootstrap()
        storage_connection_name = unique_prefix + "sbucket"
        client.create_storage_connection(storage_connection_name, "egress_bucket")

        sql = R'''
            insert into `{2}`.`{0}_{1}/` with (format={0})
            select * from AS_TABLE([<|foo:123, bar:"xxx"u|>,<|foo:456, bar:"yyy"u|>]);
            '''.format(format, yq_version, storage_connection_name)

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
        stat = json.loads(client.describe_query(query_id).result.query.statistics.json)

        graph_name = "Graph=0" if yq_version == "v1" else "Sink"
        egress_bytes = stat[graph_name]["EgressBytes"]["sum"]

        file_size = 0
        for file in bucket.objects.all():
            if file.key.startswith("{0}_{1}/".format(format, yq_version)):
                file_size += bucket.Object(file.key).content_length

        assert file_size == egress_bytes, "File size {} mistmatches egress bytes {}".format(file_size, egress_bytes)
        assert sum(kikimr.control_plane.get_metering()) == 10

    @yq_all
    @pytest.mark.parametrize("format1", ["json_list", "json_each_row", "csv_with_names", "parquet"])
    @pytest.mark.parametrize("format2", ["json_list", "json_each_row", "csv_with_names", "parquet"])
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_convert(self, kikimr, s3, client, format1, format2, yq_version, unique_prefix):
        if yq_version == 'v1':
            pytest.skip("Tiket: YQ-3004")

        resource = boto3.resource(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("convert_bucket")
        bucket.create(ACL='public-read-write')

        kikimr.control_plane.wait_bootstrap()
        storage_connection_name = unique_prefix + "sbucket"
        client.create_storage_connection(storage_connection_name, "convert_bucket")

        sql = R'''
            insert into `{3}`.`{0}_1_{1}_{2}/` with (format={0})
            select * from AS_TABLE([<|foo:123, bar:"xxx"u|>,<|foo:456, bar:"yyy"u|>]);
            '''.format(format1, format2, yq_version, storage_connection_name)

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
        stat = json.loads(client.describe_query(query_id).result.query.statistics.json)

        graph_name = "Graph=0" if yq_version == "v1" else "Sink"
        egress_bytes_1 = stat[graph_name]["EgressBytes"]["sum"]

        sql = R'''
            insert into `{3}`.`{0}_2_{1}_{2}/` with (format={1})
            select foo, bar from `{3}`.`{0}_1_{1}_{2}/*` with (format={0}, schema(
                foo Int NOT NULL,
                bar String NOT NULL
            ))
            '''.format(format1, format2, yq_version, storage_connection_name)

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
        stat = json.loads(client.describe_query(query_id).result.query.statistics.json)

        graph_name = "Graph=0" if yq_version == "v1" else "Sink"
        ingress_bytes_1 = stat[graph_name]["IngressBytes"]["sum"]
        egress_bytes_2 = stat[graph_name]["EgressBytes"]["sum"]

        sql = R'''
            select foo, bar from `{3}`.`{0}_2_{1}_{2}/*` with (format={1}, schema(
                foo Int NOT NULL,
                bar String NOT NULL
            ))
            '''.format(format1, format2, yq_version, storage_connection_name)

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
        stat = json.loads(client.describe_query(query_id).result.query.statistics.json)

        graph_name = "Graph=0" if yq_version == "v1" else "ResultSet"
        ingress_bytes_2 = stat[graph_name]["IngressBytes"]["sum"]

        file_size_1 = 0
        file_size_2 = 0
        for file in bucket.objects.all():
            if file.key.startswith("{0}_1_{1}_{2}/".format(format1, format2, yq_version)):
                file_size_1 += bucket.Object(file.key).content_length
            if file.key.startswith("{0}_2_{1}_{2}/".format(format1, format2, yq_version)):
                file_size_2 += bucket.Object(file.key).content_length

        assert file_size_1 == egress_bytes_1, "File {} size {} mistmatches egress bytes {}".format(format1, file_size_1, egress_bytes_1)
        assert file_size_2 == egress_bytes_2, "File {} size {} mistmatches egress bytes {}".format(format2, file_size_2, egress_bytes_2)
        if format1 != "parquet":
            assert file_size_1 == ingress_bytes_1, "File {} size {} mistmatches ingress bytes {}".format(format1, file_size_1, ingress_bytes_1)
        if format2 != "parquet":
            assert file_size_2 == ingress_bytes_2, "File {} size {} mistmatches ingress bytes {}".format(format2, file_size_2, ingress_bytes_2)
        assert sum(kikimr.control_plane.get_metering()) == 30

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_precompute(self, kikimr, s3, client, yq_version, unique_prefix):
        resource = boto3.resource(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("pbucket")
        bucket.create(ACL='public-read-write')

        kikimr.control_plane.wait_bootstrap()
        storage_connection_name = unique_prefix + "pb"
        client.create_storage_connection(storage_connection_name, "pbucket")

        sql = fR'''
            insert into `{storage_connection_name}`.`path1/` with (format=json_list)
            select * from AS_TABLE([<|foo:123, bar:"xxx"u|>,<|foo:456, bar:"yyy"u|>]);
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        sql = fR'''
            insert into `{storage_connection_name}`.`path2/` with (format=csv_with_names)
            select * from AS_TABLE([<|foo:123, bar:"xxx"u|>,<|foo:456, bar:"yyy"u|>]);
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        sql = fR'''
            $c1 =
                SELECT
                count(*) as `count`
                FROM
                `{storage_connection_name}`.`path1/`
                with (format=json_list, schema(
                    foo Int NOT NULL,
                    bar String NOT NULL
                ));

            $c2 =
                SELECT
                count(*) as `count`
                FROM
                `{storage_connection_name}`.`path2/`
                with (format=csv_with_names, schema(
                    foo Int NOT NULL,
                    bar String NOT NULL
                ));

            select $c1 + $c2;
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
        stat = json.loads(client.describe_query(query_id).result.query.statistics.json)

        graph_name = "Precompute=0" if yq_version == "v1" else "Precompute_0_0"
        ingress_0 = stat[graph_name]["IngressBytes"]["sum"]

        graph_name = "Precompute=1" if yq_version == "v1" else "Precompute_0_1"
        ingress_1 = stat[graph_name]["IngressBytes"]["sum"]

        ingress = ingress_0 + ingress_1

        file_size = 0
        for file in bucket.objects.all():
            file_size += bucket.Object(file.key).content_length

        assert file_size == ingress, "Total size {} mistmatches ingress bytes {}".format(file_size, ingress)
        assert sum(kikimr.control_plane.get_metering()) == 30

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_sum(self, kikimr, s3, client, yq_version, unique_prefix):
        resource = boto3.resource(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        bucket_name = "sum_bucket_{}".format(yq_version)
        bucket = resource.Bucket(bucket_name)
        bucket.create(ACL='public-read-write')

        kikimr.control_plane.wait_bootstrap()
        storage_connection_name = unique_prefix + "sbucket"
        client.create_storage_connection(storage_connection_name, bucket_name)

        sql = fR'''
            insert into `{storage_connection_name}`.`file/` with (format="csv_with_names")
            select * from AS_TABLE([<|foo:123, bar:"xxx"u|>,<|foo:456, bar:"yyy"u|>]);
            '''

        for i in range(0, 10):
            query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
            client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        files_size = 0
        for file in bucket.objects.all():
            files_size += bucket.Object(file.key).content_length

        sql = ("PRAGMA dq.MaxTasksPerStage=\"1\";" if yq_version == "v1" else "") + fR'''
            select foo, bar from `{storage_connection_name}`.`file/*` with (format="csv_with_names", schema(
                foo Int NOT NULL,
                bar String NOT NULL
            ))
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        stat = json.loads(client.describe_query(query_id).result.query.statistics.json)

        graph_name = "Graph=0" if yq_version == "v1" else "ResultSet"
        ingress_bytes = stat[graph_name]["IngressBytes"]["sum"]

        assert files_size == ingress_bytes, "Files size {} mistmatches ingress bytes {}".format(files_size, ingress_bytes)
        assert sum(kikimr.control_plane.get_metering()) == 110

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_aborted_by_user(self, kikimr, client):
        kikimr.control_plane.wait_bootstrap()

        sql = R'''
SELECT * FROM AS_TABLE(()->(Yql::ToStream(ListReplicate(<|x:
"0123456789ABCDEF"
|>, 4000000000))));
'''
        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.RUNNING)
        client.abort_query(query_id)
        client.wait_query_status(query_id, fq.QueryMeta.ABORTED_BY_USER)
        assert sum(kikimr.control_plane.get_metering()) == 10
