#!/usr/bin/env python
# -*- coding: utf-8 -*-

import boto3
import logging
import os
import pytest

from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase
import ydb.public.api.protos.ydb_value_pb2 as ydb_value
import ydb.public.api.protos.draft.fq_pb2 as fq
from ydb.tests.tools.fq_runner.kikimr_utils import yq_v1


class TestStreamingJoin(TestYdsBase):
    @yq_v1
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_grace_join(self, kikimr, s3, client, unique_prefix):
        self.init_topics("pq_test_grace_join")

        # S3
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("test_grace_join")
        bucket.create(ACL='public-read')

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        fruits = R'''Time,Fruit,Price
                    0,Banana,3
                    1,Apple,2
                    2,Pear,15'''
        s3_client.put_object(Body=fruits, Bucket='test_grace_join', Key='fruits.csv', ContentType='text/plain')

        kikimr.control_plane.wait_bootstrap(1)

        storage_connection_name = unique_prefix + "test_grace_join"
        client.create_storage_connection(storage_connection_name, "test_grace_join")

        connection_response = client.create_yds_connection(
            "myyds", os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT")
        )
        keyColumn = ydb_value.Column(name="Data", type=ydb_value.Type(type_id=ydb_value.Type.PrimitiveTypeId.STRING))
        client.create_yds_binding(
            name="my_binding_in",
            stream=self.input_topic,
            format="raw",
            connection_id=connection_response.result.connection_id,
            columns=[keyColumn],
        )
        client.create_yds_binding(
            name="my_binding_out",
            stream=self.output_topic,
            format="raw",
            connection_id=connection_response.result.connection_id,
            columns=[keyColumn],
        )

        sql = fR'''
            pragma dq.HashJoinMode="grace";

            $stream = SELECT `Data` FROM bindings.`my_binding_in`;

            $s3 = SELECT *,
            FROM `{storage_connection_name}`.`fruits.csv`
            WITH (format=csv_with_names, SCHEMA (
                Time UInt64 NOT NULL,
                Fruit String NOT NULL,
                Price Int NOT NULL
            ));

            $my_join = SELECT * FROM $stream AS a LEFT JOIN $s3 AS b ON a.Data = b.Fruit;

            INSERT INTO bindings.`my_binding_out`
            SELECT Data AS data FROM $my_join WHERE Data LIKE '%ahaha%'
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.STREAMING).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.RUNNING)
        kikimr.compute_plane.wait_zero_checkpoint(query_id)

        data = ['{{"event_class": "{}", "time": "{}"}}'.format("ahaha", "2024-01-03T00:00:00Z")]
        self.write_stream(data, self.input_topic)

        read_data = self.read_stream(1)
        logging.info("Data was read: {}".format(read_data))

        # This condition will be failed after the fix grace join for streaming queries
        assert len(read_data) == 0

        client.abort_query(query_id)

        client.wait_query(query_id)
