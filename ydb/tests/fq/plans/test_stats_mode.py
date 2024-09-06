#!/usr/bin/env python
# -*- coding: utf-8 -*-

import boto3
import logging
import pytest
import ydb.public.api.protos.draft.fq_pb2 as fq
from ydb.tests.tools.fq_runner.kikimr_utils import yq_all


class TestStatsMode:
    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    @pytest.mark.parametrize(
        "stats_mode", ["STATS_MODE_NONE", "STATS_MODE_BASIC", "STATS_MODE_FULL", "STATS_MODE_PROFILE"]
    )
    def test_mode(self, kikimr, s3, client, stats_mode):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("pbucket")
        bucket.create(ACL='public-read')
        kikimr.control_plane.wait_bootstrap(1)
        client.create_storage_connection("pb", "pbucket")

        sql = '''
            insert into pb.`path/` with (format=csv_with_names)
            select * from AS_TABLE([<|foo:1, bar:"xxx"u|>,<|foo:2, bar:"yyy"u|>]);
            '''
        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        sql = '''
            insert into pb.`path/` with (format=csv_with_names)
            select * from AS_TABLE([<|foo:3, bar:"xxx"u|>,<|foo:4, bar:"yyy"u|>]);
            '''
        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        sql = '''
            select bar, count(foo) as foo_count, sum(foo) as foo_sum
            from pb.`path/` with (format=csv_with_names, schema(
                foo Int NOT NULL,
                bar String NOT NULL
            ))
            group by bar
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        logging.debug(str(result_set))
        assert len(result_set.columns) == 3
        assert len(result_set.rows) == 2
        # assert result_set.rows[0].items[0].uint64_value == 1024 * 10
        # 1024 x 1024 x 10 = 10 MB of raw data + little overhead for header, eols etc
        # assert sum(kikimr.control_plane.get_metering(1)) == 11
