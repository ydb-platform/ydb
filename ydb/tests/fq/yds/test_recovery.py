#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import time
import multiprocessing
import pytest
import os
import random

import yatest

from ydb.tests.library.harness import param_constants
import ydb.tests.library.common.yatest_common as yatest_common
from ydb.tests.tools.fq_runner.kikimr_runner import StreamingOverKikimr
from ydb.tests.tools.fq_runner.kikimr_runner import StreamingOverKikimrConfig
from ydb.tests.tools.fq_runner.kikimr_utils import yq_v1
from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase
from ydb.tests.tools.datastreams_helpers.control_plane import create_stream

import ydb.public.api.protos.draft.fq_pb2 as fq

import library.python.retry as retry


def run_with_sleep(args):
    program_args, time_min, time_max, duration = args
    deadline = time.time() + duration
    while time.time() < deadline:
        yatest.common.execute(program_args)
        time.sleep(random.uniform(time_min, time_max))


@pytest.fixture
def kikimr():
    kikimr_conf = StreamingOverKikimrConfig(node_count=8, cloud_mode=True)
    kikimr = StreamingOverKikimr(kikimr_conf)
    kikimr.start_mvp_mock_server()
    kikimr.start()
    yield kikimr
    kikimr.stop()
    kikimr.stop_mvp_mock_server()


class TestRecovery(TestYdsBase):
    @classmethod
    def setup_class(cls):
        # for retry
        cls.retry_conf = retry.RetryConf().upto(seconds=30).waiting(0.1)

    @retry.retry_intrusive
    def get_graph_master_node_id(self, query_id):
        for node_index in self.kikimr.control_plane.kikimr_cluster.nodes:
            if self.kikimr.control_plane.get_task_count(node_index, query_id) > 0:
                return node_index
        assert False, "No active graphs found"

    def dump_workers(self, worker_count, ca_count, wait_time=yatest_common.plain_or_under_sanitizer(30, 150)):
        deadline = time.time() + wait_time
        while True:
            wcs = 0
            ccs = 0
            list = []
            for node_index in self.kikimr.control_plane.kikimr_cluster.nodes:
                wc = self.kikimr.control_plane.get_worker_count(node_index)
                cc = self.kikimr.control_plane.get_ca_count(node_index)
                wcs += wc
                ccs += cc
                list.append([node_index, wc, cc])
            if wcs == worker_count and ccs == ca_count:
                for [s, w, c] in list:
                    if w * 2 != c:
                        continue
                for [s, w, c] in list:
                    logging.debug("Node {}, workers {}, ca {}".format(s, w, c))
                return
            if time.time() > deadline:
                for [s, w, c] in list:
                    logging.debug("Node {}, workers {}, ca {}".format(s, w, c))
                assert False, "Workers={} and CAs={}, but {} and {} expected".format(wcs, ccs, worker_count, ca_count)

    @yq_v1
    def test_delete(self, client, kikimr):
        for node_index in kikimr.control_plane.kikimr_cluster.nodes:
            kikimr.control_plane.wait_bootstrap(node_index)

        self.kikimr = kikimr
        self.init_topics("recovery", partitions_count=2)

        #  Consumer and topics to create are written in ya.make file.
        sql = R'''
            PRAGMA dq.MaxTasksPerStage="2";

            INSERT INTO myyds.`{output_topic}`
            SELECT STREAM
                *
            FROM myyds.`{input_topic}`;'''.format(
            input_topic=self.input_topic,
            output_topic=self.output_topic,
        )
        client.create_yds_connection("myyds", os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"))

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.STREAMING).result.query_id
        logging.debug("Uuid = {}".format(kikimr.uuid))

        self.dump_workers(2, 4)

        client.abort_query(query_id)
        client.wait_query(query_id)

        self.dump_workers(0, 0)

    @yq_v1
    def test_program_state_recovery(self, client, kikimr):
        # 100  105   110   115   120   125   130   135   140   (ms)
        #  [ Bucket1  )           |(emited)
        #             [  Bucket2  )           |(emited)
        #                   .<------------------------------------- restart
        #                         [  Bucket3  )           |(emited)
        for node_index in kikimr.control_plane.kikimr_cluster.nodes:
            kikimr.control_plane.wait_bootstrap(node_index)
        for node_index in kikimr.control_plane.kikimr_cluster.nodes:
            kikimr.control_plane.wait_discovery(node_index)

        self.kikimr = kikimr
        self.init_topics("program_state_recovery", partitions_count=1)

        #  Consumer and topics to create are written in ya.make file.
        sql = f'''
            PRAGMA dq.MaxTasksPerStage="1";
            INSERT INTO myyds.`{self.output_topic}`
            SELECT STREAM
                Yson::SerializeText(Yson::From(TableRow()))
            FROM (
                SELECT STREAM
                    Sum(t) as sum
                FROM (
                    SELECT STREAM
                        Yson::LookupUint64(ys, "time") as t
                    FROM (
                        SELECT STREAM
                            Yson::Parse(Data) AS ys
                        FROM myyds.`{self.input_topic}`))
                GROUP BY
                    HOP(DateTime::FromMilliseconds(CAST(Unwrap(t) as Uint32)), "PT0.01S", "PT0.01S", "PT0.01S"));'''
        client.create_yds_connection("myyds", os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"))

        query_id = client.create_query(
            "test_program_state_recovery", sql, type=fq.QueryContent.QueryType.STREAMING
        ).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.RUNNING)
        logging.debug("Uuid = {}".format(kikimr.uuid))
        master_node_index = self.get_graph_master_node_id(query_id)
        logging.debug("Master node {}".format(master_node_index))
        kikimr.compute_plane.wait_zero_checkpoint(query_id)

        self.write_stream([f'{{"time" = {i};}}' for i in range(100, 115, 2)])

        kikimr.compute_plane.wait_completed_checkpoints(
            query_id, self.kikimr.compute_plane.get_completed_checkpoints(query_id) + 1
        )
        # restart node with CA
        node_to_restart = None
        for node_index in kikimr.control_plane.kikimr_cluster.nodes:
            wc = kikimr.control_plane.get_worker_count(node_index)
            if wc is not None:
                if wc > 0 and node_index != master_node_index and node_to_restart is None:
                    node_to_restart = node_index
        assert node_to_restart is not None, "Can't find any task on non master node"

        logging.debug("Restart non-master node {}".format(node_to_restart))

        kikimr.control_plane.kikimr_cluster.nodes[node_to_restart].stop()
        kikimr.control_plane.kikimr_cluster.nodes[node_to_restart].start()
        kikimr.control_plane.wait_bootstrap(node_to_restart)

        self.write_stream([f'{{"time" = {i};}}' for i in range(116, 144, 2)])

        # wait aggregated
        expected = [
            '{"sum" = 520u}',
            '{"sum" = 570u}',
            '{"sum" = 620u}',
        ]
        received = self.read_stream(3)
        assert received == expected

        client.abort_query(query_id)
        client.wait_query(query_id)

        self.dump_workers(0, 0)

    @yq_v1
    # @pytest.mark.parametrize(
    #     "restart_master",
    #     [False, True],
    #     ids=["not_master", "master"]
    # )
    def test_recovery(self, client, kikimr):
        for node_index in kikimr.control_plane.kikimr_cluster.nodes:
            kikimr.control_plane.wait_bootstrap(node_index)
        for node_index in kikimr.control_plane.kikimr_cluster.nodes:
            kikimr.control_plane.wait_discovery(node_index)

        self.init_topics("recovery", partitions_count=2)

        self.kikimr = kikimr

        #  Consumer and topics to create are written in ya.make file.
        sql = R'''
            PRAGMA dq.MaxTasksPerStage="2";

            INSERT INTO myyds.`{output_topic}`
            SELECT STREAM
                *
            FROM myyds.`{input_topic}`;'''.format(
            input_topic=self.input_topic,
            output_topic=self.output_topic,
        )
        client.create_yds_connection("myyds", os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"))
        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.STREAMING).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.RUNNING)
        kikimr.compute_plane.wait_zero_checkpoint(query_id)

        logging.debug("Uuid = {}".format(kikimr.uuid))
        master_node_index = self.get_graph_master_node_id(query_id)
        logging.debug("Master node {}".format(master_node_index))

        self.write_stream([str(i) for i in range(1, 11)])

        d = {}

        read_data = self.read_stream(10)
        assert len(read_data) == 10
        for m in read_data:
            n = int(m)
            assert n >= 1 and n <= 10
            assert n not in d
            d[n] = 1

        self.dump_workers(2, 4)

        node_to_restart = None
        for node_index in kikimr.control_plane.kikimr_cluster.nodes:
            wc = kikimr.control_plane.get_worker_count(node_index)
            if wc is not None:
                if wc > 0 and node_index != master_node_index and node_to_restart is None:
                    node_to_restart = node_index
        assert node_to_restart is not None, "Can't find any task on non master node"

        logging.debug("Restart non-master node {}".format(node_to_restart))

        kikimr.control_plane.kikimr_cluster.nodes[node_to_restart].stop()
        kikimr.control_plane.kikimr_cluster.nodes[node_to_restart].start()
        kikimr.control_plane.wait_bootstrap(node_to_restart)

        self.dump_workers(2, 4)

        self.write_stream([str(i) for i in range(11, 21)])

        read_data = self.read_stream(10)
        assert len(read_data) == 10
        for m in read_data:
            n = int(m)
            assert n >= 1 and n <= 20
            if n in d:
                d[n] = d[n] + 1
            else:
                d[n] = 1

        assert len(d) == 20

        logging.debug("Restart Master node {}".format(master_node_index))

        kikimr.control_plane.kikimr_cluster.nodes[master_node_index].stop()
        kikimr.control_plane.kikimr_cluster.nodes[master_node_index].start()
        kikimr.control_plane.wait_bootstrap(master_node_index)
        master_node_index = self.get_graph_master_node_id(query_id)

        logging.debug("New master node {}".format(master_node_index))

        self.dump_workers(2, 4)

        self.write_stream([str(i) for i in range(21, 31)])

        read_data = self.read_stream(10)
        assert len(read_data) == 10
        for m in read_data:
            n = int(m)
            assert n >= 1 and n <= 30
            if n in d:
                d[n] = d[n] + 1
            else:
                d[n] = 1
        assert len(d) == 30

        zero_checkpoints_metric = kikimr.control_plane.get_checkpoint_coordinator_metric(
            query_id, "StartedFromEmptyCheckpoint"
        )
        restored_metric = kikimr.control_plane.get_checkpoint_coordinator_metric(
            query_id, "RestoredFromSavedCheckpoint"
        )
        assert restored_metric >= 1, "RestoredFromSavedCheckpoint: {}, StartedFromEmptyCheckpoint: {}".format(
            restored_metric, zero_checkpoints_metric
        )

        client.abort_query(query_id)
        client.wait_query(query_id)

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
            yatest_common.plain_or_under_sanitizer("10ms", "50ms"),
            "--sleep-max",
            yatest_common.plain_or_under_sanitizer("100ms", "500ms"),
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
        deadline = time.time() + yatest_common.plain_or_under_sanitizer(300, 900)
        while True:
            status = client.describe_query(query_id).result.query.meta.status
            assert status == fq.QueryMeta.RUNNING, "Unexpected status " + fq.QueryMeta.ComputeStatus.Name(status)
            completed = self.kikimr.control_plane.get_completed_checkpoints(automatic_id, False)
            if completed >= 5:
                break
            assert time.time() < deadline, "Completed: {}".format(completed)
            time.sleep(yatest_common.plain_or_under_sanitizer(0.5, 2))

        close_ic_sessions_future.wait()

    @yq_v1
    def test_program_state_recovery_error_if_no_states(self, client, kikimr):
        for node_index in kikimr.control_plane.kikimr_cluster.nodes:
            kikimr.control_plane.wait_bootstrap(node_index)
        for node_index in kikimr.control_plane.kikimr_cluster.nodes:
            kikimr.control_plane.wait_discovery(node_index)
        self.init_topics("error_if_no_states", partitions_count=1)

        sql = R'''
            INSERT INTO myyds.`{output_topic}`
            SELECT STREAM * FROM myyds.`{input_topic}`;'''.format(
            input_topic=self.input_topic,
            output_topic=self.output_topic,
        )
        client.create_yds_connection("myyds", os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"))

        query_id = client.create_query(
            "error_if_no_states", sql, type=fq.QueryContent.QueryType.STREAMING
        ).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.RUNNING)
        kikimr.compute_plane.wait_zero_checkpoint(query_id)
        kikimr.compute_plane.wait_completed_checkpoints(
            query_id, kikimr.compute_plane.get_completed_checkpoints(query_id) + 1
        )

        for node_index in kikimr.control_plane.kikimr_cluster.nodes:
            kikimr.control_plane.kikimr_cluster.nodes[node_index].stop()

        session = kikimr.driver.table_client.session().create()
        checkpoint_table_prefix = "/local/CheckpointCoordinatorStorage_" + kikimr.uuid + '/states'
        session.transaction().execute(f"DELETE FROM `{checkpoint_table_prefix}`", commit_tx=True)

        for node_index in kikimr.control_plane.kikimr_cluster.nodes:
            kikimr.control_plane.kikimr_cluster.nodes[node_index].start()
        for node_index in kikimr.control_plane.kikimr_cluster.nodes:
            kikimr.control_plane.wait_bootstrap(node_index)

        client.wait_query_status(query_id, fq.QueryMeta.FAILED)
        describe_result = client.describe_query(query_id).result
        logging.debug("Describe result: {}".format(describe_result))
        describe_string = "{}".format(describe_result)
        assert r"Can\'t restore: STORAGE_ERROR" in describe_string
        assert r"Checkpoint is not found" in describe_string
