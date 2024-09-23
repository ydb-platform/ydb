#!/usr/bin/env python
# -*- coding: utf-8 -*-

import boto3
import logging
import os
import pytest
import time

from ydb.tests.tools.fq_runner.fq_client import FederatedQueryClient
from ydb.tests.tools.datastreams_helpers.control_plane import list_read_rules
from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase

import ydb.public.api.protos.draft.fq_pb2 as fq
from ydb.tests.tools.fq_runner.kikimr_utils import yq_v1


class TestEarlyFinish(TestYdsBase):
    @yq_v1
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_early_finish(self, kikimr, s3, client, unique_prefix):
        # Topics
        self.init_topics("select_early", create_output=False)
        yds_connection_name = unique_prefix + "myyds"
        client.create_yds_connection(yds_connection_name, os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"))

        # S3
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("rbucket")
        bucket.create(ACL='public-read')

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        s3_client.put_object(Body="A", Bucket='rbucket', Key='A.txt', ContentType='text/plain')
        s3_client.put_object(Body="C", Bucket='rbucket', Key='C.txt', ContentType='text/plain')
        storage_connection_name = unique_prefix + "rawbucket"
        client.create_storage_connection(storage_connection_name, "rbucket")

        sql = R'''
            SELECT S.Data as Data1, D.Data as Data2
            FROM `{yds_connection_name}`.`{input_topic}` AS S
            INNER JOIN (
                SELECT Data
                FROM `{storage_connection_name}`.`*`
                WITH (format=raw, SCHEMA (
                    Data String
                ))
                ORDER BY Data DESC
            ) AS D
            ON S.Data = D.Data
            LIMIT 2
            '''.format(
            yds_connection_name=yds_connection_name,
            storage_connection_name=storage_connection_name,
            input_topic=self.input_topic,
        )

        client = FederatedQueryClient("my_folder", streaming_over_kikimr=kikimr)

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.STREAMING).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.RUNNING)
        kikimr.control_plane.wait_zero_checkpoint(query_id)

        messages = ["A", "B", "C"]

        self.write_stream(messages[:2])
        time.sleep(5)
        self.write_stream(messages[2:])

        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        data = client.get_result_data(query_id)

        result_set = data.result.result_set
        logging.debug(str(result_set))
        assert len(result_set.rows) == 2
        assert len(result_set.columns) == 2
        assert result_set.rows[0].items[0].bytes_value == bytes(messages[0], 'UTF-8')
        assert result_set.rows[1].items[0].bytes_value == bytes(messages[2], 'UTF-8')

        # Assert that all read rules were removed after query stops
        read_rules = list_read_rules(self.input_topic)
        assert len(read_rules) == 0, read_rules

        assert self.wait_until((lambda: kikimr.control_plane.get_actor_count(1, "YQ_PINGER") == 0))
