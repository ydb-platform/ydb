#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
import multiprocessing
import pytest
import os
import random

import yatest

from ydb.tests.library.common.helpers import plain_or_under_sanitizer
from ydb.tests.library.common.wait_for import wait_for
from ydb.tests.fq.streaming_common.common import get_checkpoint_coordinator_metric
from ydb.tests.library.harness import param_constants
from ydb.tests.tools.fq_runner.kikimr_runner import StreamingOverKikimr
from ydb.tests.tools.fq_runner.kikimr_runner import StreamingOverKikimrConfig
from ydb.tests.tools.fq_runner.kikimr_runner import TenantConfig
from ydb.tests.tools.fq_runner.kikimr_utils import yq_v1
from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase
from ydb.tests.tools.datastreams_helpers.control_plane import create_stream

import ydb.public.api.protos.draft.fq_pb2 as fq


def run_with_sleep(args):
    program_args, time_min, time_max, duration = args
    deadline = time.time() + duration
    while time.time() < deadline:
        yatest.common.execute(program_args)
        time.sleep(random.uniform(time_min, time_max))


@pytest.fixture
def kikimr():
    kikimr_conf = StreamingOverKikimrConfig(
        node_count={"/cp": TenantConfig(1), "/compute": TenantConfig(2)}, cloud_mode=True
    )
    kikimr = StreamingOverKikimr(kikimr_conf)
    kikimr.start_mvp_mock_server()
    kikimr.start()
    yield kikimr
    kikimr.stop()
    kikimr.stop_mvp_mock_server()


class TestRecovery(TestYdsBase):

    def test_delete(self, ydb_topics):
        io = ydb_topics('recovery_delete', partitions=2)
        query = io.start(f"""
            PRAGMA ydb.MaxTasksPerStage = '2';
            INSERT INTO {io.output} SELECT Data FROM {io.input};
        """)
        assert io.state(query)['Status'] == 'RUNNING', io.state(query)
        assert wait_for(
            lambda: any(
                io.get_actor_count(io.kikimr, node_id, 'KQP_COMPUTE_ACTOR') > 0 for node_id in io.kikimr.cluster.slots
            ),
            timeout_seconds=120,
            step_seconds=0.5,
        ), 'The query did not create compute actors'
        io.wait_actor_count('YQ_CHECKPOINT_COORDINATOR', 1)
        io.stop(query)
        assert io.state(query)['Status'] == 'STOPPED', io.state(query)
        for activity in (
            'KQP_COMPUTE_ACTOR',
            'YQ_CHECKPOINT_COORDINATOR',
            'DQ_PQ_READ_ACTOR',
            'DQ_MESSAGE_STREAM_READ_ACTOR',
        ):
            io.wait_actor_count(activity, 0)

    @pytest.mark.parametrize('ydb_cluster', [{'distributed_compute': True}], indirect=True)
    def test_program_state_recovery(self, ydb_topics):
        # Keep the original windows and input split: the second window must
        # combine values saved before the restart with values arriving after it.
        # Two readers permit a non-coordinator failure; a fixed key preserves the original input order.
        io = ydb_topics('program_state_recovery', partitions=2)
        query = io.start(f'''
            PRAGMA ydb.MaxTasksPerStage="2";
            INSERT INTO {io.output}
            SELECT Yson::SerializeText(Yson::From(TableRow()))
            FROM (
                SELECT Sum(t) as sum
                FROM (
                    SELECT Yson::LookupUint64(ys, "time") as t
                    FROM (
                        SELECT Yson::Parse(Data) AS ys
                        FROM {io.input}))
                GROUP BY
                    HOP(DateTime::FromMilliseconds(CAST(Unwrap(t) as Uint32)), "PT0.01S", "PT0.01S", "PT0.01S"));
        ''')
        io.write([f'{{"time" = {i};}}' for i in range(100, 115, 2)], partition_key='stateful')
        io.checkpoint(query, count=3)
        io.restart_compute_node(with_coordinator=False)
        io.checkpoint(query, count=3)
        io.write([f'{{"time" = {i};}}' for i in range(116, 144, 2)], partition_key='stateful')

        expected = [
            '{"sum" = 520u}',
            '{"sum" = 570u}',
            '{"sum" = 620u}',
        ]
        assert io.read(3) == expected
        assert get_checkpoint_coordinator_metric(io.kikimr.cluster, query, 'RestoredFromSavedCheckpoint') >= 1
        io.stop(query)
        io.wait_actor_count('KQP_COMPUTE_ACTOR', 0)
        io.wait_actor_count('YQ_CHECKPOINT_COORDINATOR', 0)

    @pytest.mark.parametrize('ydb_cluster', [{'distributed_compute': True}], indirect=True)
    def test_recovery(self, ydb_topics):
        io = ydb_topics('recovery', partitions=2)
        query = io.start(f"""
            PRAGMA ydb.MaxTasksPerStage = '2';
            INSERT INTO {io.output} SELECT Data FROM {io.input};
        """)
        expected_compute_actors = sum(
            io.get_actor_count(io.kikimr, node_id, 'KQP_COMPUTE_ACTOR') for node_id in io.kikimr.cluster.slots
        )
        assert expected_compute_actors > 0
        io.wait_actor_count('YQ_CHECKPOINT_COORDINATOR', 1)
        seen = set()
        for batch in range(3):
            if batch:
                # Lose a worker outside the coordinator, then the current coordinator itself.
                io.restart_compute_node(with_coordinator=(batch == 2))
                io.checkpoint(query, count=3)
            io.wait_actor_count('KQP_COMPUTE_ACTOR', expected_compute_actors)
            io.wait_actor_count('YQ_CHECKPOINT_COORDINATOR', 1)
            expected = list(range(batch * 10 + 1, batch * 10 + 11))
            io.write([str(value) for value in expected])
            received = [int(value) for value in io.read(len(expected))]
            assert sorted(received) == expected, received
            seen.update(received)
            assert len(seen) == (batch + 1) * 10, seen
            io.checkpoint(query, count=3)

        restored = get_checkpoint_coordinator_metric(io.kikimr.cluster, query, 'RestoredFromSavedCheckpoint')
        started_empty = get_checkpoint_coordinator_metric(io.kikimr.cluster, query, 'StartedFromEmptyCheckpoint')
        assert restored >= 1, (restored, started_empty)
        assert io.state(query)['Status'] == 'RUNNING', io.state(query)
        io.stop(query)
        io.wait_actor_count('KQP_COMPUTE_ACTOR', 0)
        io.wait_actor_count('YQ_CHECKPOINT_COORDINATOR', 0)

    def close_ic_session_args(self, node1, node2):
        s1 = self.kikimr.control_plane.kikimr_cluster.nodes[node1]
        s2 = self.kikimr.control_plane.kikimr_cluster.nodes[node2]
        # action = "closepeersocket"
        # action = "poisonsession"
        action = "closeinputsession"
        return [
            param_constants.kikimr_driver_path(),
            "-s",
            "{}:{}".format(s1.host, s1.grpc_port),
            "admin",
            "debug",
            "interconnect",
            action,
            "--node",
            str(s2.node_id),
        ]

    def slowpoke_args(self, node):
        s = self.kikimr.control_plane.kikimr_cluster.nodes[node]
        return [
            param_constants.kikimr_driver_path(),
            "-s",
            "{}:{}".format(s.host, s.grpc_port),
            "admin",
            "debug",
            "interconnect",
            "slowpoke",
            "--pool-id",
            "4",
            "--duration",
            "30s",
            "--sleep-min",
            plain_or_under_sanitizer("10ms", "50ms"),
            "--sleep-max",
            plain_or_under_sanitizer("100ms", "500ms"),
            "--reschedule-min",
            "10ms",
            "--reschedule-max",
            "100ms",
            "--num-actors",
            "2",
        ]

    def start_close_ic_sessions_processes(self):
        pool = multiprocessing.Pool()
        args = []

        for node1_index in self.kikimr.control_plane.kikimr_cluster.nodes:
            yatest.common.execute(self.slowpoke_args(node1_index))
            for node2_index in self.kikimr.control_plane.kikimr_cluster.nodes:
                if node2_index > node1_index:
                    args.append((self.close_ic_session_args(node1_index, node2_index), 0.1, 2, 30))
        return pool.map_async(run_with_sleep, args)

    @yq_v1
    @pytest.mark.skip(reason="Should be tuned")
    def test_ic_disconnection(self, client):
        for node_index in self.kikimr.control_plane.kikimr_cluster.nodes:
            self.kikimr.control_plane.wait_bootstrap(node_index)
        for node_index in self.kikimr.control_plane.kikimr_cluster.nodes:
            self.kikimr.control_plane.wait_discovery(node_index)

        self.kikimr = kikimr
        self.init_topics("disconnection", partitions_count=2)
        input_topic_1 = "disconnection_i_1"
        input_topic_2 = "disconnection_i_2"
        create_stream(input_topic_1)
        create_stream(input_topic_2)

        #  Consumer and topics to create are written in ya.make file.
        sql = R'''
            PRAGMA dq.MaxTasksPerStage="42";

            INSERT INTO myyds.`{output_topic}`
                SELECT (S1.Data || S2.Data) || ""
                FROM myyds.`{input_topic_1}` AS S1
                INNER JOIN (SELECT * FROM myyds.`{input_topic_2}`) AS S2
                ON S1.Data = S2.Data
        '''.format(
            input_topic_1=input_topic_1,
            input_topic_2=input_topic_2,
            output_topic=self.output_topic,
        )

        close_ic_sessions_future = self.start_close_ic_sessions_processes()

        folder_id = "my_folder"
        # automatic query will not clean up metrics after failure
        client.create_yds_connection("myyds", os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"))
        query_id = client.create_query(
            "disconnected", sql, type=fq.QueryContent.QueryType.STREAMING, automatic=True
        ).result.query_id
        automatic_id = "automatic_" + folder_id

        client.wait_query_status(query_id, fq.QueryMeta.RUNNING)

        # Checkpointing must be finished
        deadline = time.time() + plain_or_under_sanitizer(300, 900)
        while True:
            status = client.describe_query(query_id).result.query.meta.status
            assert status == fq.QueryMeta.RUNNING, "Unexpected status " + fq.QueryMeta.ComputeStatus.Name(status)
            completed = self.kikimr.control_plane.get_completed_checkpoints(automatic_id, False)
            if completed >= 5:
                break
            assert time.time() < deadline, "Completed: {}".format(completed)
            time.sleep(plain_or_under_sanitizer(0.5, 2))

        close_ic_sessions_future.wait()

    def test_program_state_recovery_error_if_no_states(self, ydb_topics):
        io = ydb_topics('error_if_no_states')
        client = io.kikimr.ydb_client
        query = io.start(f"""
            INSERT INTO {io.output} SELECT Data FROM {io.input};
        """)
        io.write(['before_restart'])
        assert io.read(1) == ['before_restart']
        io.checkpoint(query, count=3)
        execution_id = client.query(f"""
            SELECT LastExecutionId FROM `.sys/streaming_queries` WHERE Path = '{query}';
        """)[0].rows[0]['LastExecutionId']
        assert execution_id
        checkpoint_id = execution_id + '-' + query

        # Stop checkpoint writers before removing state. The tenant must stay up
        # to execute the DELETE against its own metadata tables.
        io.stop(query)
        io.wait_actor_count('YQ_CHECKPOINT_COORDINATOR', 0)
        io.wait_actor_count('KQP_COMPUTE_ACTOR', 0)

        def checkpoint_rows(table):
            return client.query(f"""
                SELECT COUNT(*) AS saved FROM `.metadata/streaming/checkpoints/{table}`
                WHERE graph_id = '{checkpoint_id}';
            """)[0].rows[0]['saved']

        assert checkpoint_rows('states') > 0
        assert checkpoint_rows('checkpoints_metadata') > 0
        client.query(f"""
            DELETE FROM `.metadata/streaming/checkpoints/states`
            WHERE graph_id = '{checkpoint_id}';
        """)
        assert checkpoint_rows('states') == 0
        # Keep checkpoint metadata: recovery must find the checkpoint and fail
        # to load its missing task states rather than start with empty state.
        assert checkpoint_rows('checkpoints_metadata') > 0

        for node_id in io.kikimr.cluster.slots:
            io.restart_node(io.kikimr, node_id)
        timeout = plain_or_under_sanitizer(120, 500)
        io.kikimr.wait_kqp_node_count(len(io.kikimr.cluster.slots), timeout_seconds=timeout)
        client.query(f'ALTER STREAMING QUERY `{query}` SET (RUN = TRUE);')
        last_state = None

        def restore_failed():
            nonlocal last_state
            rows = client.query(f"""
                SELECT Status, Issues, RetryCount
                FROM `.sys/streaming_queries` WHERE Path = '{query}';
            """)[0].rows
            assert len(rows) == 1, rows
            last_state = rows[0]
            issues = last_state['Issues']
            return (
                "Can't restore: STORAGE_ERROR" in issues
                and 'Checkpoint is not found' in issues
                and 'will be restarted' in issues
                and last_state['RetryCount'] > 0
            )

        # Streaming queries retry storage failures instead of becoming terminal FAILED.
        assert wait_for(restore_failed, timeout_seconds=timeout, step_seconds=0.5), last_state
        assert last_state['Status'] in ('RUNNING', 'SUSPENDED'), last_state
        io.stop(query)
