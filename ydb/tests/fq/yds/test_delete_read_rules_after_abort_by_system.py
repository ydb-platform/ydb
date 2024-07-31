#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os

from test_base import TestBaseWithAbortingConfigParams

from ydb.tests.tools.datastreams_helpers.control_plane import list_read_rules
from ydb.tests.tools.fq_runner.fq_client import FederatedQueryClient
from ydb.tests.tools.fq_runner.kikimr_utils import yq_v1
import ydb.public.api.protos.draft.fq_pb2 as fq


class TestDeleteReadRulesAfterAbortBySystem(TestBaseWithAbortingConfigParams):
    @yq_v1
    def test_delete_read_rules_after_abort_by_system(self):
        topic_name = "read_rules_leaking"
        conn = "yds_0"
        self.init_topics(topic_name)

        sql = R'''
            PRAGMA dq.MaxTasksPerStage="5";

            INSERT INTO {conn}.`{output_topic}`
            SELECT * FROM {conn}.`{input_topic}`;'''.format(
            input_topic=self.input_topic, output_topic=self.output_topic, conn=conn
        )

        client = FederatedQueryClient("my_folder", streaming_over_kikimr=self.streaming_over_kikimr)

        client.create_yds_connection(conn, os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"))
        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.STREAMING).result.query_id

        client.wait_query_status(query_id, fq.QueryMeta.RUNNING)
        self.streaming_over_kikimr.compute_plane.wait_zero_checkpoint(query_id)

        read_rules = list_read_rules(self.input_topic)
        assert len(read_rules) == 1, read_rules

        for _ in range(5):
            self.streaming_over_kikimr.compute_plane.stop()
            self.streaming_over_kikimr.compute_plane.start()
            get_task_count = self.streaming_over_kikimr.control_plane.get_request_count(1, "GetTask")
            while get_task_count == self.streaming_over_kikimr.control_plane.get_request_count(1, "GetTask"):
                pass
            if client.describe_query(query_id).result.query.meta.status == fq.QueryMeta.ABORTED_BY_SYSTEM:
                break
        else:
            assert False, "Query was NOT aborted"
        # client.wait_query_status(query_id, fq.QueryMeta.ABORTED_BY_SYSTEM)

        read_rules = list_read_rules(self.input_topic)
        assert len(read_rules) == 0, read_rules
