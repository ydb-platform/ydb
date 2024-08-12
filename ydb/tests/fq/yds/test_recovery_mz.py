#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import time
import pytest
import random
import os
import yatest

import ydb.tests.library.common.yatest_common as yatest_common
from ydb.tests.tools.fq_runner.kikimr_runner import StreamingOverKikimr
from ydb.tests.tools.fq_runner.kikimr_runner import StreamingOverKikimrConfig
from ydb.tests.tools.fq_runner.kikimr_runner import TenantConfig
from ydb.tests.tools.fq_runner.kikimr_utils import yq_v1
from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase

import library.python.retry as retry
import ydb.public.api.protos.draft.fq_pb2 as fq


@pytest.fixture
def kikimr():
    kikimr_conf = StreamingOverKikimrConfig(
        cloud_mode=True, node_count={"/cp": TenantConfig(1), "/compute": TenantConfig(8)}
    )
    kikimr = StreamingOverKikimr(kikimr_conf)
    # control
    kikimr.control_plane.fq_config['control_plane_storage']['mapping'] = {"common_tenant_name": ["/compute"]}
    kikimr.control_plane.fq_config['control_plane_storage']['task_lease_retry_policy'] = {}
    kikimr.control_plane.fq_config['control_plane_storage']['task_lease_retry_policy']['retry_count'] = 5
    kikimr.control_plane.fq_config['control_plane_storage']['task_lease_retry_policy']['retry_period'] = "30s"
    kikimr.control_plane.fq_config['control_plane_storage']['task_lease_ttl'] = "3s"
    # compute
    kikimr.compute_plane.fq_config['pinger']['ping_period'] = "1s"
    kikimr.start_mvp_mock_server()
    kikimr.start()
    yield kikimr
    kikimr.stop()
    kikimr.stop_mvp_mock_server()


def run_with_sleep(args):
    program_args, time_min, time_max, duration = args
    deadline = time.time() + duration
    while time.time() < deadline:
        yatest.common.execute(program_args)
        time.sleep(random.uniform(time_min, time_max))


class TestRecovery(TestYdsBase):
    @retry.retry_intrusive
    def get_graph_master_node_id(self, query_id):
        for node_index in self.kikimr.compute_plane.kikimr_cluster.nodes:
            if self.kikimr.compute_plane.get_task_count(node_index, query_id) > 0:
                return node_index
        assert False, "No active graphs found"

    def dump_workers(self, worker_count, ca_count, wait_time=yatest_common.plain_or_under_sanitizer(30, 150)):
        deadline = time.time() + wait_time
        while True:
            wcs = 0
            ccs = 0
            list = []
            for node_index in self.kikimr.compute_plane.kikimr_cluster.nodes:
                wc = self.kikimr.compute_plane.get_worker_count(node_index)
                cc = self.kikimr.compute_plane.get_ca_count(node_index)
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
    def test_recovery(self, kikimr, client, yq_version):
        self.init_topics(f"pq_kikimr_streaming_{yq_version}", partitions_count=2)

        self.retry_conf = retry.RetryConf().upto(seconds=30).waiting(0.1)
        self.kikimr = kikimr
        kikimr.compute_plane.wait_bootstrap()
        kikimr.compute_plane.wait_discovery()

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
        self.kikimr.compute_plane.wait_zero_checkpoint(query_id)

        logging.debug("Uuid = {}".format(kikimr.uuid))
        master_node_index = self.get_graph_master_node_id(query_id)
        logging.debug("Master node {}".format(master_node_index))

        self.write_stream([str(i) for i in range(1, 11)])

        read_data = self.read_stream(10)

        for message in read_data:
            logging.info("Received message: {}".format(message))

        assert len(read_data) == 10

        d = {}
        for m in read_data:
            n = int(m)
            assert n >= 1 and n <= 10
            assert n not in d
            d[n] = 1

        self.dump_workers(2, 4)

        node_to_restart = None
        for node_index in kikimr.compute_plane.kikimr_cluster.nodes:
            wc = kikimr.compute_plane.get_worker_count(node_index)
            if wc is not None:
                if wc > 0 and node_index != master_node_index and node_to_restart is None:
                    node_to_restart = node_index
        assert node_to_restart is not None, "Can't find any task on non master node"

        logging.debug("Restart non-master node {}".format(node_to_restart))

        kikimr.compute_plane.kikimr_cluster.nodes[node_to_restart].stop()
        kikimr.compute_plane.kikimr_cluster.nodes[node_to_restart].start()
        kikimr.compute_plane.wait_bootstrap(node_to_restart)

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

        logging.debug("Restart Master node {}".format(master_node_index))

        kikimr.compute_plane.kikimr_cluster.nodes[master_node_index].stop()
        kikimr.compute_plane.kikimr_cluster.nodes[master_node_index].start()
        kikimr.compute_plane.wait_bootstrap(master_node_index)
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

        zero_checkpoints_metric = kikimr.compute_plane.get_checkpoint_coordinator_metric(
            query_id, "StartedFromEmptyCheckpoint"
        )
        restored_metric = kikimr.compute_plane.get_checkpoint_coordinator_metric(
            query_id, "RestoredFromSavedCheckpoint"
        )
        assert restored_metric >= 1, "RestoredFromSavedCheckpoint: {}, StartedFromEmptyCheckpoint: {}".format(
            restored_metric, zero_checkpoints_metric
        )

        client.abort_query(query_id)
        client.wait_query(query_id)
