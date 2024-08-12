#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import os
import pytest
import time

import ydb.public.api.protos.draft.fq_pb2 as fq
import ydb.tests.library.common.yatest_common as yatest_common
from ydb.tests.tools.fq_runner.fq_client import StreamingDisposition
from ydb.tests.tools.fq_runner.kikimr_utils import yq_v1
from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase
from ydb.tests.tools.datastreams_helpers.data_plane import write_stream, read_stream


def assert_issues_contain(text, issues):
    for issue in issues:
        if text in issue.message:
            return
    assert False, f"Text [{text}] is expected to be among issues:\n{issues}"


class TestContinueMode(TestYdsBase):
    @yq_v1
    @pytest.mark.parametrize("mvp_external_ydb_endpoint", [{"endpoint": os.getenv("YDB_ENDPOINT")}], indirect=True)
    def test_continue_from_offsets(self, kikimr, client):
        client.create_yds_connection(name="yds", database_id="FakeDatabaseId")

        self.init_topics("continue_1", partitions_count=2)
        input_topic_1 = self.input_topic
        output_topic_1 = self.output_topic
        consumer_1 = self.consumer_name
        self.init_topics("continue_2", partitions_count=2)
        input_topic_2 = self.input_topic
        output_topic_2 = self.output_topic
        consumer_2 = self.consumer_name

        partition_key = "trololo"

        sql = Rf'''
            PRAGMA dq.MaxTasksPerStage="2";

            INSERT INTO yds.`{output_topic_1}`
            SELECT Data FROM yds.`{input_topic_1}`;'''

        query_id = client.create_query(
            "continue-from-offsets-query", sql, type=fq.QueryContent.QueryType.STREAMING
        ).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.RUNNING)
        kikimr.compute_plane.wait_zero_checkpoint(query_id)

        data = ["1", "2"]
        write_stream(input_topic_1, data, partition_key=partition_key)

        assert read_stream(output_topic_1, len(data), consumer_name=consumer_1) == data

        kikimr.compute_plane.wait_completed_checkpoints(
            query_id, kikimr.compute_plane.get_completed_checkpoints(query_id) + 1
        )

        client.abort_query(query_id)
        client.wait_query(query_id)

        # Write to first topic.
        # These offsets wouldn't be fresh but we should get this data in second output topic
        # because offsets should be restored from checkpoint
        data_2 = ["3", "4"]
        write_stream(input_topic_1, data_2, partition_key=partition_key)

        sql2 = Rf'''
            PRAGMA dq.MaxTasksPerStage="1";

            INSERT INTO yds.`{output_topic_2}`
            SELECT Data FROM yds.`{input_topic_1}`;

            INSERT INTO yds.`{output_topic_2}`
            SELECT Data FROM yds.`{input_topic_2}`;'''

        def assert_has_saved_checkpoints():
            describe_result = client.describe_query(query_id).result
            logging.debug("Describe result: {}".format(describe_result))
            assert describe_result.query.meta.has_saved_checkpoints, "Expected has_saved_checkpoints flag: {}".format(
                describe_result.query.meta
            )

        def find_text(issues, text):
            for issue in issues:
                if text in issue.message:
                    return True
                if len(issue.issues) > 0 and find_text(issue.issues, text):
                    return True
            return False

        def assert_has_issues(text, transient=False, deadline=0):
            describe_result = client.describe_query(query_id).result
            logging.debug("Describe result: {}".format(describe_result))
            if find_text(describe_result.query.transient_issue if transient else describe_result.query.issue, text):
                return True
            assert (
                deadline and deadline > time.time()
            ), "Text \"{}\" is expected to be found in{} issues, but was not found. Describe query result:\n{}".format(
                text, " transient" if transient else "", describe_result
            )
            return False

        assert_has_saved_checkpoints()

        # 1. Not forced mode. Expect to fail
        client.modify_query(
            query_id,
            "continue-from-offsets-query",
            sql2,
            type=fq.QueryContent.QueryType.STREAMING,
            state_load_mode=fq.StateLoadMode.EMPTY,
            streaming_disposition=StreamingDisposition.from_last_checkpoint(),
        )
        client.wait_query_status(query_id, fq.QueryMeta.FAILED)
        assert_has_issues(
            "Topic `continue_2_input` is not found in previous query. Use force mode to ignore this issue"
        )

        # 2. Forced mode. Expect to run.
        client.modify_query(
            query_id,
            "continue-from-offsets-query",
            sql2,
            type=fq.QueryContent.QueryType.STREAMING,
            state_load_mode=fq.StateLoadMode.EMPTY,
            streaming_disposition=StreamingDisposition.from_last_checkpoint(True),
        )
        client.wait_query_status(query_id, fq.QueryMeta.RUNNING)
        transient_issues_deadline = time.time() + yatest_common.plain_or_under_sanitizer(60, 300)
        msg = "Topic `continue_2_input` is not found in previous query. Query will use fresh offsets for its partitions"
        while True:
            if assert_has_issues(msg, True, transient_issues_deadline):
                break
            else:
                time.sleep(0.3)
        kikimr.compute_plane.wait_completed_checkpoints(
            query_id, kikimr.compute_plane.get_completed_checkpoints(query_id) + 1
        )

        restored_metric = kikimr.compute_plane.get_checkpoint_coordinator_metric(
            query_id, "RestoredStreamingOffsetsFromCheckpoint"
        )
        assert restored_metric == 1

        data_3 = ["5", "6"]
        write_stream(input_topic_2, data_3, partition_key=partition_key)

        assert sorted(read_stream(output_topic_2, 4, consumer_name=consumer_2)) == ["3", "4", "5", "6"]

        kikimr.compute_plane.wait_completed_checkpoints(
            query_id, kikimr.compute_plane.get_completed_checkpoints(query_id) + 1
        )

        assert_has_saved_checkpoints()

        # Check that after node failure the query will restore from its usual checkpoint
        kikimr.compute_plane.kikimr_cluster.nodes[1].stop()
        kikimr.compute_plane.kikimr_cluster.nodes[1].start()
        kikimr.compute_plane.wait_bootstrap(1)

        # sleep task lease ttl / 2
        time.sleep(2.5)

        # Wait while graph is restored from checkpoint
        n = 100
        while n:
            n -= 1
            restored_metric = kikimr.compute_plane.get_checkpoint_coordinator_metric(
                query_id, "RestoredFromSavedCheckpoint", expect_counters_exist=False
            )
            if restored_metric >= 1:
                break
            time.sleep(0.3)
        assert restored_metric >= 1

        assert_has_saved_checkpoints()

    @yq_v1
    @pytest.mark.parametrize("mvp_external_ydb_endpoint", [{"endpoint": os.getenv("YDB_ENDPOINT")}], indirect=True)
    def test_deny_disposition_from_checkpoint_in_create_query(self, client):
        client.create_yds_connection(name="yds_create", database_id="FakeDatabaseId")

        self.init_topics("deny_disposition_from_checkpoint_in_create_query")

        sql = Rf'''
            PRAGMA dq.MaxTasksPerStage="2";

            INSERT INTO yds_create.`{self.output_topic}`
            SELECT Data FROM yds_create.`{self.input_topic}`;'''

        response = client.create_query(
            "deny_disposition_from_checkpoint_in_create_query",
            sql,
            type=fq.QueryContent.QueryType.STREAMING,
            streaming_disposition=StreamingDisposition.from_last_checkpoint(),
            check_issues=False,
        )
        assert response.issues
        assert_issues_contain(
            "Streaming disposition \"from_last_checkpoint\" is not allowed in CreateQuery request", response.issues
        )

    @yq_v1
    @pytest.mark.parametrize("mvp_external_ydb_endpoint", [{"endpoint": os.getenv("YDB_ENDPOINT")}], indirect=True)
    def test_deny_state_load_mode_from_checkpoint_in_modify_query(self, kikimr, client):
        client.create_yds_connection(name="yds_modify", database_id="FakeDatabaseId")

        self.init_topics("deny_state_load_mode_from_checkpoint_in_modify_query")

        sql = Rf'''
            PRAGMA dq.MaxTasksPerStage="2";

            INSERT INTO yds_modify.`{self.output_topic}`
            SELECT Data FROM yds_modify.`{self.input_topic}`;'''

        response = client.create_query(
            "deny_state_load_mode_from_checkpoint_in_modify_query", sql, type=fq.QueryContent.QueryType.STREAMING
        )

        query_id = response.result.query_id

        client.wait_query_status(query_id, fq.QueryMeta.RUNNING)
        kikimr.compute_plane.wait_zero_checkpoint(query_id)

        client.abort_query(query_id)
        client.wait_query(query_id)

        response = client.modify_query(
            query_id,
            "deny_state_load_mode_from_checkpoint_in_modify_query",
            sql,
            type=fq.QueryContent.QueryType.STREAMING,
            state_load_mode=fq.StateLoadMode.FROM_LAST_CHECKPOINT,
            streaming_disposition=StreamingDisposition.from_last_checkpoint(),
            check_issues=False,
        )

        assert response.issues
        assert_issues_contain("State load mode \"FROM_LAST_CHECKPOINT\" is not supported", response.issues)
