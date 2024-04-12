#!/usr/bin/env python
# -*- coding: utf-8 -*-

import boto3

import pytest

from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase

import ydb.public.api.protos.draft.fq_pb2 as fq
from ydb.tests.tools.fq_runner.kikimr_utils import yq_all


class TestS3(TestYdsBase):
    @yq_all
    @pytest.mark.parametrize("limit", [5, 10, 15, 20, 100, 1000])
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_size_limit(self, kikimr, s3, client, limit, unique_prefix):
        resource = boto3.resource(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("fbucket")
        bucket.create(ACL='public-read')

        s3_client = boto3.client(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key"
        )

        info = "Да и ты, читатель, разве ты не Ничья Рыба и одновременно разве не Рыба на Лине?"
        s3_client.put_object(Body=info, Bucket='fbucket', Key='info.txt', ContentType='text/plain')
        kikimr.control_plane.wait_bootstrap(1)
        storage_connection_name = unique_prefix + "test-connection"
        client.create_storage_connection(storage_connection_name, "fbucket")

        sql = R'''
                SELECT
                    data
                FROM
                    `{}`.`info.txt`
                WITH(
                read_max_bytes="{}",
                format=raw,
                SCHEMA (data String,
                    ))
            '''.format(storage_connection_name, limit)

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        result = result_set.rows[0].items[0].bytes_value
        assert result == info.encode()[:limit]
