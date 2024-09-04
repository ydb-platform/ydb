#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import os
import pytest
import time

from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase
from ydb.tests.tools.fq_runner.kikimr_utils import yq_v1

import ydb.tests.library.common.yatest_common as yatest_common
from ydb.tests.tools.datastreams_helpers.control_plane import create_read_rule, list_read_rules

import ydb.public.api.protos.draft.fq_pb2 as fq


class TestStop(TestYdsBase):
    @yq_v1
    @pytest.mark.parametrize(
        "query_type",
        [fq.QueryContent.QueryType.ANALYTICS, fq.QueryContent.QueryType.STREAMING],
        ids=["analytics", "streaming"],
    )
    def test_stop_query(self, kikimr, client, query_type):
        self.init_topics("select_stop_" + str(query_type), create_output=False)

        connection = "myyds_" + str(query_type)
        # TBD auto-create consumer and read_rule in analytics query
        if type == fq.QueryContent.QueryType.STREAMING:
            sql = R'''
                SELECT * FROM {connection}.`{input_topic}` LIMIT 3;
                '''.format(
                connection=connection,
                input_topic=self.input_topic,
            )
        else:
            create_read_rule(self.input_topic, self.consumer_name)
            sql = R'''
                PRAGMA pq.Consumer="{consumer_name}";
                SELECT * FROM {connection}.`{input_topic}` LIMIT 3;
                '''.format(
                consumer_name=self.consumer_name,
                connection=connection,
                input_topic=self.input_topic,
            )

        assert self.wait_until((lambda: kikimr.compute_plane.get_actor_count(1, "YQ_PINGER") == 0))

        client.create_yds_connection(connection, os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"))
        query_id = client.create_query("simple", sql, type=query_type).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.RUNNING)

        assert self.wait_until((lambda: kikimr.compute_plane.get_actor_count(1, "YQ_PINGER") == 1))

        time.sleep(2)  # Workaround race between write and read "from now". Remove when YQ-589 will be done.
        messages = ["A", "B"]
        self.write_stream(messages)

        deadline = time.time() + yatest_common.plain_or_under_sanitizer(30, 150)
        while True:
            if time.time() > deadline:
                break
            describe_response = client.describe_query(query_id)
            status = describe_response.result.query.meta.status
            assert status in [fq.QueryMeta.STARTING, fq.QueryMeta.RUNNING], fq.QueryMeta.ComputeStatus.Name(status)

        client.abort_query(query_id)

        client.wait_query(query_id)

        # TODO: remove this if
        if type == fq.QueryContent.QueryType.STREAMING:
            # Assert that all read rules were removed after query stops
            read_rules = list_read_rules(self.input_topic)
            assert len(read_rules) == 0, read_rules

        describe_response = client.describe_query(query_id)
        status = describe_response.result.query.meta.status
        assert status == fq.QueryMeta.ABORTED_BY_USER, fq.QueryMeta.ComputeStatus.Name(status)

        assert self.wait_until((lambda: kikimr.compute_plane.get_actor_count(1, "YQ_PINGER") == 0))

        # TBD implement and check analytics partial results as well
        if type == fq.QueryContent.QueryType.STREAMING:
            data = client.get_result_data(query_id)

            result_set = data.result.result_set
            logging.debug(str(result_set))
            assert len(result_set.rows) == 2
            assert len(result_set.columns) == 1
            assert result_set.rows[0].items[0].bytes_value == messages[0]
            assert result_set.rows[1].items[0].bytes_value == messages[1]
