#!/usr/bin/env python
# -*- coding: utf-8 -*-

import boto3
import logging
import pytest
import ydb
from ydb import convert


class TestStatsMode:
    @pytest.mark.skip(reason="YDB Query Service does not provide an SVG query timeline")
    @pytest.mark.parametrize(
        "stats_mode", ["STATS_MODE_NONE", "STATS_MODE_BASIC", "STATS_MODE_FULL", "STATS_MODE_PROFILE"]
    )
    def test_mode(self, s3, client, stats_mode):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("pbucket")
        bucket.create(ACL='public-read')
        client.query(f'''CREATE EXTERNAL DATA SOURCE pb WITH (
            SOURCE_TYPE="ObjectStorage", LOCATION="{s3.s3_url}/pbucket/", AUTH_METHOD="NONE"
        );''')

        def execute(sql):
            with client.session_pool.checkout() as session:
                mode = getattr(ydb.QueryStatsMode, stats_mode.removeprefix("STATS_MODE_"))
                with session.execute(sql, stats_mode=mode) as stream:
                    result_sets = convert.aggregate_result_sets_by_index(stream)
                return result_sets, session.last_query_stats

        sql = '''
            insert into pb.`path/` with (format=csv_with_names)
            select * from AS_TABLE([<|foo:1, bar:"xxx"u|>,<|foo:2, bar:"yyy"u|>]);
            '''
        execute(sql)

        sql = '''
            insert into pb.`path/` with (format=csv_with_names)
            select * from AS_TABLE([<|foo:3, bar:"xxx"u|>,<|foo:4, bar:"yyy"u|>]);
            '''
        execute(sql)

        sql = '''
            select bar, count(foo) as foo_count, sum(foo) as foo_sum
            from pb.`path/` with (format=csv_with_names, schema(
                foo Int NOT NULL,
                bar String NOT NULL
            ))
            group by bar
            '''

        result_sets, stats = execute(sql)
        if stats_mode in ("STATS_MODE_FULL", "STATS_MODE_PROFILE"):
            assert stats is not None
            assert stats.query_plan
        # The SVG timeline assertion can be restored when Query Service exposes it.
        result_set = result_sets[0]
        logging.debug(str(result_set))
        assert len(result_set.columns) == 3
        assert len(result_set.rows) == 2
        assert sorted(tuple(row[i] for i in range(3)) for row in result_set.rows) == [(b"xxx", 2, 4), (b"yyy", 2, 6)]
