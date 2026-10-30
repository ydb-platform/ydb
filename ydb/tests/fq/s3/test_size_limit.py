#!/usr/bin/env python
# -*- coding: utf-8 -*-

import boto3

import pytest


class TestS3:
    @pytest.mark.parametrize("runtime_listing", ["false", "true"])
    @pytest.mark.parametrize("kikimr_params", [{"raw": 20}, {"raw": 150}, {"raw": 1000}], indirect=True)
    @pytest.mark.parametrize("limit", [5, 100, 500])
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
        storage_connection_name = client.path(unique_prefix + "test-connection")
        client.create_external_data_source(storage_connection_name, "fbucket")

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

        query = client.execute(sql)

        expected_success = True
        if kikimr_params.param['raw'] < min(len(info), limit):
            expected_success = False
        query.check(expected_success)

        if expected_success:
            data = query.result_set()
            result_set = data
            result = result_set.rows[0][0]
            assert result == info.encode()[:limit]
