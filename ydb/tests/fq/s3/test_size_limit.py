#!/usr/bin/env python
# -*- coding: utf-8 -*-

import boto3

import pytest

from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase

import ydb.public.api.protos.draft.fq_pb2 as fq
from ydb.tests.tools.fq_runner.kikimr_utils import yq_all


class TestS3(TestYdsBase):
    @yq_all
    @pytest.mark.parametrize("runtime_listing", ["false", "true"])
    @pytest.mark.parametrize("kikimr_params", [{"raw": 20}, {"raw": 150}, {"raw": 1000}], indirect=True)
    @pytest.mark.parametrize("limit", [5, 100, 500])
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_size_limit(self, kikimr, s3, client, limit, kikimr_params, runtime_listing, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("fbucket")
        bucket.create(ACL='public-read')

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        info = """
            01234567890123456789012345678901234567890123456789
            01234567890123456789012345678901234567890123456789
            01234567890123456789012345678901234567890123456789
            01234567890123456789012345678901234567890123456789
        """

        s3_client.put_object(Body=info, Bucket='fbucket', Key='info.txt', ContentType='text/plain')
        kikimr.control_plane.wait_bootstrap(1)
        storage_connection_name = unique_prefix + "test-connection"
        client.create_storage_connection(storage_connection_name, "fbucket")

        sql = f'''
                pragma s3.UseRuntimeListing="{runtime_listing}";
                SELECT
                    data
                FROM
                    `{storage_connection_name}`.`info.txt`
                WITH(
                read_max_bytes="{limit}",
                format=raw,
                SCHEMA (data String,
                    ))
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id

        expected_status = fq.QueryMeta.COMPLETED
        if kikimr_params.param['raw'] < min(len(info), limit):
            expected_status = fq.QueryMeta.FAILED
        client.wait_query_status(query_id, expected_status)

        if expected_status == fq.QueryMeta.COMPLETED:
            data = client.get_result_data(query_id)
            result_set = data.result.result_set
            result = result_set.rows[0].items[0].bytes_value
            assert result == info.encode()[:limit]
