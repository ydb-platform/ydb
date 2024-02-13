#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest
import logging
import os
import time

import ydb.tests.library.common.yatest_common as yatest_common
from ydb.tests.tools.fq_runner.kikimr_runner import StreamingOverKikimr
from ydb.tests.tools.fq_runner.kikimr_runner import StreamingOverKikimrConfig
import library.python.retry as retry
from ydb.tests.tools.fq_runner.kikimr_utils import yq_v1
from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase
import ydb.public.api.protos.draft.fq_pb2 as fq


@pytest.fixture
def kikimr(request):
    kikimr_conf = StreamingOverKikimrConfig(cloud_mode=True, node_count=2)
    kikimr = StreamingOverKikimr(kikimr_conf)
    kikimr.start_mvp_mock_server()
    kikimr.start()
    yield kikimr
    kikimr.stop_mvp_mock_server()
    kikimr.stop()


class TestRecoveryMatchRecognize(TestYdsBase):

    @classmethod
    def setup_class(cls):
        # for retry
        cls.retry_conf = retry.RetryConf().upto(seconds=30).waiting(0.1)

    @retry.retry_intrusive
    def get_graph_master_node_id(self, kikimr, query_id):
        for node_index in kikimr.control_plane.kikimr_cluster.nodes:
            if kikimr.control_plane.get_task_count(node_index, query_id) > 0:
                return node_index
        assert False, "No active graphs found"

    def get_ca_count(self, kikimr, node_index):
        result = kikimr.control_plane.get_sensors(node_index, "utils").find_sensor(
            {"activity": "DQ_COMPUTE_ACTOR", "sensor": "ActorsAliveByActivity", "execpool": "User"}
        )
        return result if result is not None else 0

    def dump_workers(self, kikimr, worker_count, ca_count, wait_time=yatest_common.plain_or_under_sanitizer(30, 150)):
        deadline = time.time() + wait_time
        while True:
            wcs = 0
            ccs = 0
            list = []
            for node_index in kikimr.control_plane.kikimr_cluster.nodes:
                wc = kikimr.control_plane.get_worker_count(node_index)
                cc = self.get_ca_count(kikimr, node_index)
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

    def restart_node(self, kikimr, query_id):
        # restart node with CA


        # master_node_index = self.get_graph_master_node_id(kikimr, query_id)
        # logging.debug("Master node {}".format(master_node_index))

        node_to_restart = None

        for node_index in kikimr.compute_plane.kikimr_cluster.nodes:
            wc = kikimr.compute_plane.get_worker_count(node_index)
            if wc is not None:
                if wc > 0 and node_to_restart is None:
                    node_to_restart = node_index
        assert node_to_restart is not None, "Can't find any task on node"

        logging.debug("Restart compute node {}".format(node_to_restart))

        kikimr.compute_plane.kikimr_cluster.nodes[node_to_restart].stop()
        kikimr.compute_plane.kikimr_cluster.nodes[node_to_restart].start()
        kikimr.compute_plane.wait_bootstrap(node_to_restart)

    @yq_v1
    @pytest.mark.parametrize("kikimr", [(None, None, None)], indirect=["kikimr"])
    def test_program_state_recovery(self, kikimr, client, yq_version):

        self.init_topics(f"pq_kikimr_streaming_{yq_version}")

        sql = R'''
            PRAGMA dq.MaxTasksPerStage="2";

            pragma FeatureR010="prototype";
            pragma config.flags("TimeOrderRecoverDelay", "-1000000");
            pragma config.flags("TimeOrderRecoverAhead", "1000000");

            INSERT INTO myyds.`{output_topic}`
            SELECT ToBytes(Unwrap(Json::SerializeJson(Yson::From(TableRow()))))
            FROM (SELECT * FROM myyds.`{input_topic}`
                WITH (
                    format=json_each_row,
                    SCHEMA
                    (
                        dt UINT64
                    )))
            MATCH_RECOGNIZE(
                ORDER BY CAST(dt as Timestamp)
                MEASURES
                LAST(ALL_TRUE.dt) as dt
                ONE ROW PER MATCH
                PATTERN ( ALL_TRUE )
                DEFINE
                    ALL_TRUE as True)''' \
            .format(
            input_topic=self.input_topic,
            output_topic=self.output_topic,
        )

        client.create_yds_connection("myyds", os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"))
        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.STREAMING).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.RUNNING)
        kikimr.compute_plane.wait_zero_checkpoint(query_id)

        master_node_index = self.get_graph_master_node_id(kikimr, query_id)
        logging.debug("Master node {}".format(master_node_index))

        messages1 = ['{"dt": 1696849942400002}', '{"dt": 1696849942000001}']
        self.write_stream(messages1)

        logging.debug("get_completed_checkpoints {}".format(kikimr.compute_plane.get_completed_checkpoints(query_id)))
        kikimr.compute_plane.wait_completed_checkpoints(
            query_id, kikimr.compute_plane.get_completed_checkpoints(query_id) + 1
        )

        self.restart_node(kikimr, query_id)

        messages2 = [
            '{"dt": 1696849942800000}',
            '{"dt": 1696849943200003}',
            '{"dt": 1696849943300003}',
            '{"dt": 1696849943600003}',
            '{"dt": 1696849943900003}'
        ]
        self.write_stream(messages2)

        assert client.get_query_status(query_id) == fq.QueryMeta.RUNNING

        expected = ['{"dt":1696849942000001}', '{"dt":1696849942400002}', '{"dt":1696849942800000}']

        read_data = self.read_stream(len(expected))
        logging.info("Data was read: {}".format(read_data))

        assert read_data == expected

        client.abort_query(query_id)
        client.wait_query(query_id)

        self.dump_workers(kikimr, 0, 0)


    @yq_v1
    @pytest.mark.parametrize("kikimr", [(None, None, None)], indirect=["kikimr"])
    def test_match_recognize(self, kikimr, client, yq_version):

        self.init_topics("test_match_recognize_save_load_state")

        sql = R'''
            PRAGMA dq.MaxTasksPerStage="2";

            pragma FeatureR010="prototype";
            pragma config.flags("TimeOrderRecoverDelay", "-1000000");
            pragma config.flags("TimeOrderRecoverAhead", "1000000");

            INSERT INTO myyds.`{output_topic}`
            SELECT ToBytes(Unwrap(Json::SerializeJson(Yson::From(TableRow()))))
            FROM (SELECT * FROM myyds.`{input_topic}`
                WITH (
                    format=json_each_row,
                    SCHEMA
                    (
                        dt UINT64,
                        str STRING
                    )))
            MATCH_RECOGNIZE(
                ORDER BY CAST(dt as Timestamp)
                MEASURES
                   LAST(A.dt) as dt_begin,
                   LAST(C.dt) as dt_end,
                   LAST(A.str) as a_str,
                   LAST(B.str) as b_str,
                   LAST(C.str) as c_str
                ONE ROW PER MATCH
                PATTERN ( A B C )
                DEFINE
                    A as A.str='A',
                    B as B.str='B',
                    C as C.str='C')''' \
            .format(
            input_topic=self.input_topic,
            output_topic=self.output_topic,
        )

        client.create_yds_connection("myyds", os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"))
        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.STREAMING).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.RUNNING)
        kikimr.compute_plane.wait_zero_checkpoint(query_id)

        messages1 = [
            '{"dt": 1696849942000001, "str": "A" }',
            '{"dt": 1696849942500001, "str": "B" }',
            '{"dt": 1696849943000001, "str": "C" }',
            '{"dt": 1696849943600001, "str": "D" }']       # push A+B from TimeOrderRecoverer to MatchRecognize 
        self.write_stream(messages1)

        # A + B : in MatchRecognize
        # C + D : in TimeOrderRecoverer

        logging.debug("get_completed_checkpoints {}".format(kikimr.compute_plane.get_completed_checkpoints(query_id)))
        kikimr.compute_plane.wait_completed_checkpoints(
            query_id, kikimr.compute_plane.get_completed_checkpoints(query_id) + 1
        )

        self.restart_node(kikimr, query_id)

        self.write_stream(['{"dt": 1696849944100001, "str": "E" }'])

        assert client.get_query_status(query_id) == fq.QueryMeta.RUNNING

        expected = ['{"a_str":"A","b_str":"B","c_str":"C","dt_begin":1696849942000001,"dt_end":1696849943000001}']

        read_data = self.read_stream(1)
        logging.info("Data was read: {}".format(read_data))

        assert read_data == expected

        client.abort_query(query_id)
        client.wait_query(query_id)

        self.dump_workers(kikimr, 0, 0)
