#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import logging
import os
import pytest
import time

import ydb.tests.library.common.yatest_common as yatest_common
from ydb.tests.tools.fq_runner.kikimr_utils import yq_v1
from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase

from ydb.tests.tools.datastreams_helpers.control_plane import create_stream
from ydb.tests.tools.datastreams_helpers.data_plane import write_stream

import ydb.public.api.protos.draft.fq_pb2 as fq


class TestMemAlloc(TestYdsBase):
    @yq_v1
    def test_join_alloc(self, kikimr, client):
        pytest.skip("This test is not ready yet")

        self.init_topics("select_join_alloc", create_output=False)
        create_stream("joined_topic")

        sql = R'''
            SELECT S1.Data as Data1, S2.Data as Data2, ListReplicate(S2.Data, 1000000) as Data20
            FROM myyds.`{input_topic}` AS S1
            INNER JOIN (SELECT * FROM myyds.`{joined_topic}` LIMIT 2) AS S2
            ON S1.Data = S2.Data
            LIMIT 2
            '''.format(
            input_topic=self.input_topic, joined_topic="joined_topic"
        )

        client.create_yds_connection("myyds", os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"))
        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.STREAMING).result.query_id

        time.sleep(2)  # Workaround race between write and read "from now". Remove when YQ-589 will be done.
        messages = ["A", "B", "C"]
        self.write_stream(messages)
        write_stream("joined_topic", messages)

        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        logging.debug(client.get_result_data(query_id))
        st = json.loads(client.describe_query(query_id).result.query.statistics.json)
        logging.debug(st["Graph=1"]["TaskRunner"]["Stage=Total"]["MkqlMaxMemoryUsage"]["count"])

    @yq_v1
    def test_hop_alloc(self, kikimr, client):
        pytest.skip("This test is not ready yet")

        self.init_topics("select_hop_alloc", create_output=False)

        sql = R'''
            --SELECT COUNT(*)
            --FROM myyds.`{input_topic}`
            --GROUP BY HOP(Just(CurrentUtcTimestamp()), "PT10S", "PT10S", "PT10S"), Data
            --LIMIT 1
            SELECT 1
            '''.format(
            input_topic=self.input_topic,
        )

        client.create_yds_connection("myyds", os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"))
        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.STREAMING).result.query_id

        time.sleep(2)  # Workaround race between write and read "from now". Remove when YQ-589 will be done.
        for i in range(1):
            self.write_stream([format(i, "0x")])
        time.sleep(yatest_common.plain_or_under_sanitizer(15, 60))

        client.abort_query(query_id)
        client.wait_query_status(query_id, fq.QueryMeta.ABORTED_BY_USER)

        logging.debug(client.get_result_data(query_id))
        st = json.loads(client.describe_query(query_id).result.query.statistics.json)
        logging.debug(st["Graph=1"]["TaskRunner"]["Stage=Total"]["MkqlMaxMemoryUsage"]["count"])
