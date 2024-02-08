#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import time

import ydb.tests.library.common.yatest_common as yatest_common

import ydb.public.api.protos.draft.fq_pb2 as fq
from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase
from ydb.tests.tools.fq_runner.kikimr_utils import yq_v1


class TestBigState(TestYdsBase):
    @yq_v1
    def test_gt_8mb(self, kikimr, client):

        self.init_topics("select_hop_8mb", create_output=False)

        sql = R'''
            $s = SELECT ListConcat(ListReplicate(Data, 9000000)) as Data2
            FROM myyds.`{input_topic}`
            WITH SCHEMA (Data String NOT NULL);

            SELECT * from $s
            GROUP BY HOP(Just(CurrentUtcTimestamp()), "PT1S", "PT1S", "PT1S"), Data2
            LIMIT 1
            ''' \
            .format(
            input_topic=self.input_topic
        )

        client.create_yds_connection("myyds", os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"))
        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.STREAMING).result.query_id
        kikimr.compute_plane.wait_zero_checkpoint(query_id)

        for _ in range(5):
            messages = ["A", "B", "C"]
            self.write_stream(messages)
            time.sleep(1)

        deadline = time.time() + yatest_common.plain_or_under_sanitizer(60, 300)
        while time.time() < deadline:
            aborted_checkpoints = kikimr.compute_plane.get_checkpoint_coordinator_metric(query_id, "AbortedCheckpoints")
            if not aborted_checkpoints:
                time.sleep(0.5)

        assert aborted_checkpoints != 0
