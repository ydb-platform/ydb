#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pathlib import Path

import pytest
import ydb as ydb_sdk

from ydb.tests.fq.streaming_common.common import StreamingTestBase, get_checkpoint_coordinator_metric
from ydb.tests.library.common.wait_for import wait_for


class TestContinueMode(StreamingTestBase):
    def test_continue_from_offsets(self, ydb_topics):
        first = ydb_topics('continue_1', partitions=2)
        second = ydb_topics('continue_2', partitions=2)
        client = first.kikimr.ydb_client
        partition_key = 'trololo'
        query = first.start(f"""
            PRAGMA ydb.MaxTasksPerStage = '2';
            INSERT INTO {first.output} SELECT Data FROM {first.input};
        """)
        try:
            execution_id = client.query(f"""
                SELECT LastExecutionId FROM `.sys/streaming_queries` WHERE Path = '{query}';
            """)[0].rows[0]['LastExecutionId']
            assert execution_id
            checkpoint_id = execution_id + '-' + query

            def assert_has_saved_checkpoints():
                # The checkpoint namespace survives ALTER and execution-id changes.
                rows = client.query(f"""
                    SELECT COUNT(*) AS saved
                    FROM `.metadata/streaming/checkpoints/checkpoints_metadata`
                    WHERE graph_id = '{checkpoint_id}';
                """)[0].rows
                assert rows[0]['saved'] > 0, rows

            def metric(name):
                return get_checkpoint_coordinator_metric(first.kikimr.cluster, query, name)

            first.write(['1', '2'], partition_key=partition_key)
            assert first.read(2) == ['1', '2']
            # A checkpoint already in flight when output is observed may not yet
            # contain the delivered input offsets. Wait for subsequent checkpoints.
            first.checkpoint(query, count=3)
            first.stop(query)
            assert_has_saved_checkpoints()

            first.write(['3', '4'], partition_key=partition_key)
            # This source is absent from the old checkpoint. Forced continuation
            # must start it at fresh offsets, skipping data already in the topic.
            second.write(['before-new-source-start'], partition_key=partition_key)
            sql = f"""
                PRAGMA ydb.MaxTasksPerStage = '1';
                INSERT INTO {second.output}
                SELECT Data FROM {first.input}
                UNION ALL
                SELECT Data FROM {second.input};
            """
            missing_topic = f'Topic `{second.input_topic}` is not found in previous query'

            # FORCE permits replacing the SQL text. The disposition independently
            # controls whether missing sources in the old checkpoint are tolerated.
            with pytest.raises(ydb_sdk.issues.Error) as error:
                client.query(f"""
                    ALTER STREAMING QUERY `{query}` SET (
                        RUN = TRUE, FORCE = TRUE, STREAMING_DISPOSITION = FROM_CHECKPOINT
                    ) AS DO BEGIN
                        {sql}
                    END DO;
                """)
            assert missing_topic in str(error.value), str(error.value)

            def failed_with_missing_topic():
                state = first.state(query)
                return state['Status'] == 'FAILED' and missing_topic in state['Issues']

            assert wait_for(failed_with_missing_topic, timeout_seconds=120, step_seconds=0.5), first.state(query)
            first.stop(query)
            assert_has_saved_checkpoints()

            client.query(f"""
                ALTER STREAMING QUERY `{query}` SET (
                    RUN = TRUE, FORCE = TRUE, STREAMING_DISPOSITION = FROM_CHECKPOINT_FORCE
                ) AS DO BEGIN
                    {sql}
                END DO;
            """)
            first.checkpoint(query, count=3)
            assert first.state(query)['Status'] == 'RUNNING', first.state(query)

            # KQP currently logs coordinator transient issues rather than exposing
            # this warning in .sys/streaming_queries.Issues.
            warning = missing_topic + '. Query will use fresh offsets for its partitions'

            def has_fresh_offsets_warning():
                return any(
                    warning in Path(node.ydbd_log_file_path).read_text(errors='replace')
                    for node in first.kikimr.cluster.slots.values()
                )

            assert wait_for(has_fresh_offsets_warning, timeout_seconds=120, step_seconds=0.5), warning
            assert metric('RestoredStreamingOffsetsFromCheckpoint') == 1

            second.write(['5', '6'], partition_key=partition_key)
            assert sorted(second.read(4)) == ['3', '4', '5', '6']
            first.checkpoint(query, count=3)
            assert_has_saved_checkpoints()

            first.restart_streaming_node(first.kikimr)
            assert wait_for(
                lambda: metric('RestoredFromSavedCheckpoint') >= 1,
                timeout_seconds=120,
                step_seconds=0.5,
            ), 'The query did not restore from its saved checkpoint after restart'
            first.checkpoint(query, count=3)
            assert_has_saved_checkpoints()

            # Verify both sources keep working without replaying earlier messages.
            first.write(['7', '8'], partition_key=partition_key)
            second.write(['9', '10'], partition_key=partition_key)
            assert sorted(second.read(4)) == sorted(['7', '8', '9', '10'])
            first.checkpoint(query, count=3)
            assert_has_saved_checkpoints()
        finally:
            # Both helpers must keep their topics and data sources until the query is dropped.
            client.query(f'DROP STREAMING QUERY IF EXISTS `{query}`;')

    @pytest.mark.parametrize('disposition', ['FROM_CHECKPOINT', 'FROM_CHECKPOINT_FORCE'])
    def test_disposition_from_checkpoint_in_create_query(self, ydb_topics, disposition):
        io = ydb_topics('create_from_checkpoint')
        io.write(['before_create'])
        # KQP accepts checkpoint disposition on CREATE; without a previous
        # execution it starts from fresh offsets. YQ rejected this API request.
        query = io.start(f'INSERT INTO {io.output} SELECT Data FROM {io.input};', disposition=disposition)
        io.write(['after_create'])
        assert io.read(1) == ['after_create']
        io.stop(query)
