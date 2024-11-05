#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import os
import pytest
import time

from ydb.tests.tools.datastreams_helpers.control_plane import create_read_rule

import ydb.public.api.protos.draft.fq_pb2 as fq
from ydb.tests.tools.fq_runner.kikimr_utils import yq_v1


class TestSelectLimit(object):
    @yq_v1
    def test_select_same(self, kikimr, client):
        pytest.skip("Skip until streaming disposition is implemented YQ-589")

        self.init_topics("select_same", create_output=False)

        sql = R'''
            SELECT * FROM yds1.`{input_topic}` LIMIT 2;
            SELECT * FROM yds1.`{input_topic}` LIMIT 2;
            '''.format(
            input_topic=self.input_topic,
        )

        client.create_yds_connection("yds1", os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"))
        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.STREAMING).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.RUNNING)
        kikimr.wait_zero_checkpoint(query_id)

        time.sleep(2)  # Workaround race between write and read "from now". Remove when YQ-589 will be done.
        messages = ["A", "B", "C", "D", "E"]
        self.write_stream(messages)

        client.wait_query(query_id, 30)

        # by default selects use different consumers, so they will read the same
        # messages - first 2 of them

        rs = client.get_result_data(query_id, result_set_index=0).result.result_set
        logging.debug(str(rs))
        assert len(rs.rows) == 2
        assert len(rs.columns) == 1
        assert rs.rows[0].items[0].bytes_value == messages[0]
        assert rs.rows[1].items[0].bytes_value == messages[1]

        rs = client.get_result_data(query_id, result_set_index=1).result.result_set
        logging.debug(str(rs))
        assert len(rs.rows) == 2
        assert len(rs.columns) == 1
        assert rs.rows[0].items[0].bytes_value == messages[0]
        assert rs.rows[1].items[0].bytes_value == messages[1]

    @yq_v1
    def test_select_sequence(self, kikimr, client):
        pytest.skip("does not work as expected, need attention")
        self.init_topics("select_sequence", create_output=False)

        create_read_rule(self.input_topic, self.consumer_name)
        sql = R'''
            PRAGMA pq.Consumer="{consumer_name}";
            SELECT * FROM yds2.`{input_topic}` LIMIT 2;
            SELECT * FROM yds2.`{input_topic}` LIMIT 2;
            '''.format(
            consumer_name=self.consumer_name,
            input_topic=self.input_topic,
        )

        client.create_yds_connection("yds2", os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"))
        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.STREAMING).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.RUNNING)
        kikimr.wait_zero_checkpoint(query_id)

        time.sleep(2)  # Workaround race between write and read "from now". Remove when YQ-589 will be done.
        messages = ["A", "B", "C", "D", "E"]
        self.write_stream(messages)

        client.wait_query(query_id, 30)

        # explicit consumer will be used for each query, so second select will
        # fetch different set of next messages

        rs = client.get_result_data(query_id, result_set_index=0).result.result_set
        logging.debug(str(rs))
        assert len(rs.rows) == 2
        assert len(rs.columns) == 1
        assert rs.rows[0].items[0].bytes_value == messages[0]
        assert rs.rows[1].items[0].bytes_value == messages[1]

        rs = client.get_result_data(query_id, result_set_index=1).result.result_set
        logging.debug(str(rs))
        assert len(rs.rows) == 2
        assert len(rs.columns) == 1
        assert rs.rows[0].items[0].bytes_value == messages[2]
        assert rs.rows[1].items[0].bytes_value == messages[3]
