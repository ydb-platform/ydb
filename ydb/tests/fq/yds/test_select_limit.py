#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import os

from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase
from ydb.tests.tools.fq_runner.kikimr_utils import yq_v1
from ydb.tests.tools.datastreams_helpers.control_plane import list_read_rules

import ydb.public.api.protos.draft.fq_pb2 as fq


class TestSelectLimit(TestYdsBase):
    @yq_v1
    def test_select_limit(self, kikimr, client):
        self.init_topics("select_limit", create_output=False)

        sql = R'''
            SELECT * FROM myyds.`{input_topic}` LIMIT 2;
            '''.format(
            input_topic=self.input_topic,
        )

        client.create_yds_connection("myyds", os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"))
        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.STREAMING).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.RUNNING)
        kikimr.compute_plane.wait_zero_checkpoint(query_id)

        messages = [b'A', b'B', b'C']
        self.write_stream(messages)

        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        data = client.get_result_data(query_id)

        result_set = data.result.result_set
        logging.debug(str(result_set))
        assert len(result_set.rows) == 2
        assert len(result_set.columns) == 1
        assert result_set.rows[0].items[0].bytes_value == messages[0]
        assert result_set.rows[1].items[0].bytes_value == messages[1]

        # Assert that all read rules were removed after query stops
        read_rules = list_read_rules(self.input_topic)
        assert len(read_rules) == 0, read_rules
