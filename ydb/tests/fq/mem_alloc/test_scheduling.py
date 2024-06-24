#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os

import pytest
import time

import ydb.tests.library.common.yatest_common as yatest_common
from ydb.tests.tools.fq_runner.fq_client import FederatedQueryClient
from ydb.tests.tools.fq_runner.kikimr_runner import StreamingOverKikimr
from ydb.tests.tools.fq_runner.kikimr_runner import StreamingOverKikimrConfig

import ydb.public.api.protos.draft.fq_pb2 as fq

K = 1024
M = 1024 * 1024
G = 1024 * 1024 * 1024
DEFAULT_LIMIT = 8 * G
DEFAULT_DELTA = 30 * M
DEFAULT_WAIT_TIME = yatest_common.plain_or_under_sanitizer(10, 50)
LONG_WAIT_TIME = yatest_common.plain_or_under_sanitizer(60, 300)


@pytest.fixture
def kikimr(request):
    (initial, total, step) = request.param
    kikimr_conf = StreamingOverKikimrConfig(cloud_mode=True)
    kikimr = StreamingOverKikimr(kikimr_conf)
    kikimr.compute_plane.fq_config['resource_manager']['mkql_initial_memory_limit'] = initial
    kikimr.compute_plane.fq_config['resource_manager']['mkql_total_memory_limit'] = total
    kikimr.compute_plane.fq_config['resource_manager']['mkql_alloc_size'] = step
    kikimr.start_mvp_mock_server()
    kikimr.start()
    yield kikimr
    kikimr.stop_mvp_mock_server()
    kikimr.stop()


def wait_until(predicate, wait_time=DEFAULT_WAIT_TIME, wait_step=yatest_common.plain_or_under_sanitizer(0.5, 2)):
    deadline = time.time() + wait_time
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(wait_step)
    else:
        return False


def feq(a, b):
    if abs(a) <= 1 * G:
        return a == b
    else:
        return abs((a - b) / a) < 0.0000001


class TestSchedule(object):
    @pytest.mark.parametrize("kikimr", [(1 * M, 6 * M, 1 * M)], indirect=["kikimr"])
    @pytest.mark.skip(reason="Should be refactored")
    def test_skip_busy(self, kikimr):
        kikimr.wait_bootstrap()
        kikimr.kikimr_cluster.nodes[2].stop()

        self.init_topics("select_skip_busy", create_output=False)

        sql = R'''
            SELECT COUNT(*)
            FROM myyds.`{input_topic}`
            GROUP BY HOP(Just(CurrentUtcTimestamp()), "PT10S", "PT10S", "PT10S"), Data
            LIMIT 1
            '''.format(
            input_topic=self.input_topic,
        )

        client = FederatedQueryClient("my_folder", streaming_over_kikimr=kikimr)

        client.create_yds_connection("myyds", os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"))

        nodes = [1, 3, 4, 5, 6, 7, 8]

        for node_index in nodes:
            n = kikimr.get_sensors(node_index, "yq").find_sensor(
                {"subsystem": "node_manager", "sensor": "NodesHealthCheckOk"}
            )
            wait_until(
                lambda: kikimr.get_sensors(node_index, "yq").find_sensor(
                    {"subsystem": "node_manager", "sensor": "NodesHealthCheckOk"}
                )
                > n,
                wait_time=LONG_WAIT_TIME,
            )
            wait_until(lambda: kikimr.get_peer_count(node_index) == len(nodes) - 1, wait_time=LONG_WAIT_TIME)

            assert kikimr.get_mkql_limit(node_index) == kikimr.mkql_total_memory_limit, "Incorrect Limit"
            assert kikimr.get_mkql_allocated(node_index) == 0, "Incorrect Alloc"

        queries = []
        task_count = 0
        memory_per_graph = None
        tasks_per_graph = 0

        while True:
            query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.STREAMING).result.query_id
            client.wait_query_status(query_id, fq.QueryMeta.RUNNING)
            assert wait_until(
                (lambda: sum(kikimr.get_task_count(n, query_id) for n in nodes) > 0)
            ), "TaskController not started"
            task_count += sum(kikimr.get_task_count(n, query_id) for n in nodes)
            allocated = task_count * kikimr.mkql_initial_memory_limit
            assert wait_until(
                (lambda: sum(kikimr.get_mkql_allocated(n) for n in nodes) == allocated)
            ), "Task memory was not allocated"
            queries.append(query_id)
            if memory_per_graph is None:
                memory_per_graph = allocated
                tasks_per_graph = task_count
            if kikimr.mkql_total_memory_limit < allocated + memory_per_graph:
                break

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.STREAMING).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.RUNNING)

        def not_enough_memory():
            issues = client.describe_query(query_id).result.query.transient_issue
            if len(issues) == 0:
                return False
            assert len(issues) == 1, "Too many issues " + str(issues)
            assert issues[0].message == "Not enough memory to allocate tasks" and issues[0].issue_code == 6001, (
                "Incorrect issue " + issues[0].message
            )
            return True

        assert wait_until(not_enough_memory), "Allocation was not failed"
        assert (
            sum(kikimr.get_mkql_allocated(n) for n in nodes) == task_count * kikimr.mkql_initial_memory_limit
        ), "Incorrect allocation size"
        client.abort_query(query_id)
        # query is respawned every 30s, so wait with increased timeout
        # we may be lucky to stop the query, or it is stopped automatically due to high failure rate
        client.wait_query(query_id, 60, [fq.QueryMeta.ABORTED_BY_USER, fq.QueryMeta.ABORTED_BY_SYSTEM])

        kikimr.kikimr_cluster.nodes[2].start()
        for node_index in kikimr.kikimr_cluster.nodes:
            wait_until(lambda: kikimr.get_peer_count(1) == 8 - 1, wait_time=LONG_WAIT_TIME)

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.STREAMING).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.RUNNING)

        wait_until(lambda: kikimr.get_task_count(None, query_id) == tasks_per_graph)
        assert (
            kikimr.get_mkql_allocated() == (task_count + tasks_per_graph) * kikimr.mkql_initial_memory_limit
        ), "Incorrect allocation size"

        for q in queries:
            client.abort_query(q)
        client.abort_query(query_id)
