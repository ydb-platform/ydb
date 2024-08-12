#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import pytest

from ydb.tests.tools.fq_runner.kikimr_utils import yq_v1
from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase

from ydb.tests.tools.datastreams_helpers.control_plane import list_read_rules
import ydb.tests.library.common.yatest_common as yatest_common
import ydb.public.api.protos.draft.fq_pb2 as fq


class TestReadRulesDeletion(TestYdsBase):
    @yq_v1
    @pytest.mark.parametrize("with_recovery", [False, True], ids=["simple", "with_recovery"])
    def test_delete_read_rules(self, kikimr, client, with_recovery):
        topic_name = "delete_read_rules_{}".format(1 if with_recovery else 0)
        conn = "yds_{}".format(1 if with_recovery else 0)
        self.init_topics(topic_name)

        sql = R'''
            PRAGMA dq.MaxTasksPerStage="5";

            INSERT INTO {conn}.`{output_topic}`
            SELECT * FROM {conn}.`{input_topic}`;'''.format(
            input_topic=self.input_topic, output_topic=self.output_topic, conn=conn
        )

        client.create_yds_connection(conn, os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"))
        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.STREAMING).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.RUNNING)
        kikimr.compute_plane.wait_zero_checkpoint(query_id)

        data = [
            '42',
            '42',
            '42',
            '42',
            '42',
            '42',
            '42',
        ]

        self.write_stream(data)

        self.read_stream(len(data))

        read_rules = list_read_rules(self.input_topic)
        assert len(read_rules) == 1, read_rules

        if with_recovery:
            kikimr.compute_plane.kikimr_cluster.nodes[1].stop()
            kikimr.compute_plane.kikimr_cluster.nodes[1].start()
            kikimr.compute_plane.wait_bootstrap(1)

        client.abort_query(query_id)
        client.wait_query_status(
            query_id, fq.QueryMeta.ABORTED_BY_USER, timeout=yatest_common.plain_or_under_sanitizer(60, 300)
        )

        # Assert that all read rules were removed after query stops
        read_rules = list_read_rules(self.input_topic)
        assert len(read_rules) == 0, read_rules
