#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os

import pytest
import six
import time

from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase
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


@pytest.fixture
def kikimr(request):
    (initial, total, step, hard_limit) = request.param
    kikimr_conf = StreamingOverKikimrConfig(cloud_mode=True)
    kikimr = StreamingOverKikimr(kikimr_conf)
    kikimr.mkql_initial_memory_limit = initial
    kikimr.mkql_total_memory_limit = total
    kikimr.mkql_alloc_size = step
    kikimr.mkql_task_hard_memory_limit = hard_limit
    kikimr.compute_plane.fq_config['resource_manager']['mkql_initial_memory_limit'] = kikimr.mkql_initial_memory_limit
    kikimr.compute_plane.fq_config['resource_manager']['mkql_total_memory_limit'] = kikimr.mkql_total_memory_limit
    kikimr.compute_plane.fq_config['resource_manager']['mkql_alloc_size'] = kikimr.mkql_alloc_size
    kikimr.compute_plane.fq_config['resource_manager'][
        'mkql_task_hard_memory_limit'
    ] = kikimr.mkql_task_hard_memory_limit
    kikimr.control_plane.fq_config['quotas_manager']['quotas'] = []
    kikimr.control_plane.fq_config['quotas_manager']['quotas'].append(
        {
            "subject_type": "cloud",
            "subject_id": "my_cloud",
            "limit": [{"name": "yq.streamingQuery.count", "limit": 100}],
        }
    )
    kikimr.start_mvp_mock_server()
    kikimr.start()
    yield kikimr
    kikimr.stop_mvp_mock_server()
    kikimr.stop()


def wait_until(
    predicate,
    wait_time=yatest_common.plain_or_under_sanitizer(10, 50),
    wait_step=yatest_common.plain_or_under_sanitizer(0.5, 2),
):
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


class TestAlloc(TestYdsBase):
    @pytest.mark.parametrize("kikimr", [(None, None, None, None)], indirect=["kikimr"])
    def test_default_limits(self, kikimr):
        kikimr.control_plane.wait_bootstrap(1)
        assert kikimr.control_plane.get_mkql_limit(1) == 0, "Incorrect Limit"
        assert kikimr.control_plane.get_mkql_allocated(1) == 0, "Incorrect Alloc"

        self.init_topics("select_default_limits", create_output=False)

        sql = R'''
            SELECT * FROM myyds.`{input_topic}`
            '''.format(
            input_topic=self.input_topic,
        )

        client = FederatedQueryClient("my_folder", streaming_over_kikimr=kikimr)

        client.create_yds_connection("myyds", os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"))
        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.STREAMING).result.query_id

        client.wait_query_status(query_id, fq.QueryMeta.RUNNING)
        kikimr.control_plane.wait_zero_checkpoint(query_id)

        task_count = kikimr.control_plane.get_task_count(1, query_id)
        assert feq(kikimr.control_plane.get_mkql_allocated(1), task_count * DEFAULT_LIMIT), "Incorrect Alloc"

        limit = sum(
            v
            for k, v in six.iteritems(
                kikimr.control_plane.get_sensors(1, "dq_tasks").find_sensors(
                    {"operation": query_id, "subsystem": "mkql", "sensor": "MemoryLimit"}, "id"
                )
            )
        )
        usage = sum(
            v
            for k, v in six.iteritems(
                kikimr.control_plane.get_sensors(1, "dq_tasks").find_sensors(
                    {"operation": query_id, "subsystem": "mkql", "sensor": "MemoryUsage"}, "id"
                )
            )
        )
        assert limit is not None
        assert usage is not None
        # assert limit == task_count * DEFAULT_LIMIT, "Incorrect Alloc"

        client.abort_query(query_id)
        client.wait_query_status(query_id, fq.QueryMeta.ABORTED_BY_USER)
        assert kikimr.control_plane.get_mkql_allocated(1) == 0, "Incorrect Alloc"

    @pytest.mark.parametrize("kikimr", [(1 * M, 8 * G, None, None)], indirect=["kikimr"])
    def test_default_delta(self, kikimr):
        kikimr.control_plane.wait_bootstrap(1)
        assert kikimr.control_plane.get_mkql_limit(1) == kikimr.mkql_total_memory_limit, "Incorrect Limit"
        assert kikimr.control_plane.get_mkql_allocated(1) == 0, "Incorrect Alloc"

        self.init_topics("select_default_delta", create_output=False)

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
        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.STREAMING).result.query_id

        client.wait_query_status(query_id, fq.QueryMeta.RUNNING)
        kikimr.control_plane.wait_zero_checkpoint(query_id)

        task_count = kikimr.control_plane.get_task_count(1, query_id)
        initially_allocated = kikimr.control_plane.get_mkql_allocated(1)
        assert initially_allocated == task_count * kikimr.mkql_initial_memory_limit, "Incorrect Alloc"

        for i in range(10000):
            self.write_stream([format(i, "0x")])
            last_allocated = kikimr.control_plane.get_mkql_allocated(1)
            if last_allocated != initially_allocated:
                assert last_allocated == initially_allocated + DEFAULT_DELTA
                break
        else:
            assert False, "Limit was not increased"

        client.abort_query(query_id)
        client.wait_query_status(query_id, fq.QueryMeta.ABORTED_BY_USER)
        assert kikimr.control_plane.get_mkql_allocated(1) == 0, "Incorrect Alloc"

    @pytest.mark.parametrize("kikimr", [(1 * M, 24 * M, 1 * M, None)], indirect=["kikimr"])
    def test_node_limit(self, kikimr):
        kikimr.control_plane.wait_bootstrap(1)
        assert kikimr.control_plane.get_mkql_limit(1) == kikimr.mkql_total_memory_limit, "Incorrect Limit"
        assert kikimr.control_plane.get_mkql_allocated() == 0, "Incorrect Alloc"

        self.init_topics("select_node_limit", create_output=False)

        sql = R'''
            SELECT COUNT(*)
            FROM myyds.`{input_topic}`
            GROUP BY HOP(Just(CurrentUtcTimestamp()), "PT10S", "PT10S", "PT10S"), Data
            LIMIT 1
            '''.format(
            input_topic=self.input_topic,
        )

        client = FederatedQueryClient("my_folder@my_cloud", streaming_over_kikimr=kikimr)

        client.create_yds_connection("myyds", os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"))

        queries = []
        task_count = 0
        memory_per_graph = None

        while True:
            query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.STREAMING).result.query_id
            client.wait_query_status(query_id, fq.QueryMeta.RUNNING)
            assert wait_until(
                (lambda: kikimr.control_plane.get_task_count(1, query_id) > 0)
            ), "TaskController not started"
            task_count += kikimr.control_plane.get_task_count(1, query_id)
            allocated = task_count * kikimr.mkql_initial_memory_limit
            assert wait_until(
                (lambda: kikimr.control_plane.get_mkql_allocated(1) == allocated)
            ), "Task memory was not allocated"
            queries.append(query_id)
            if memory_per_graph is None:
                memory_per_graph = allocated
            if kikimr.mkql_total_memory_limit < allocated + memory_per_graph:
                break

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.STREAMING).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.RUNNING)

        def not_enough_memory():
            issues = client.describe_query(query_id).result.query.transient_issue
            if len(issues) == 0:
                return False
            # it is possible to get several similar issues
            for issue in issues:
                if issue.message == "Not enough free memory in the cluster" and issue.issue_code == 6001:
                    return True
            assert False, "Incorrect issues " + str(issues)

        assert wait_until(not_enough_memory), "Allocation was not failed"
        assert (
            kikimr.control_plane.get_mkql_allocated(1) == task_count * kikimr.mkql_initial_memory_limit
        ), "Incorrect allocation size"
        client.abort_query(query_id)
        # query is respawned every 30s, so wait with increased timeout
        # we may be lucky to stop the query, or it is stopped automatically due to high failure rate
        client.wait_query(query_id, 60, [fq.QueryMeta.ABORTED_BY_USER, fq.QueryMeta.ABORTED_BY_SYSTEM])

        task_count_0 = kikimr.control_plane.get_task_count(1, queries[0])
        assert task_count_0 > 0, "Strange query stat " + queries[0]
        client.abort_query(queries[0])
        client.wait_query_status(queries[0], fq.QueryMeta.ABORTED_BY_USER)

        assert wait_until(
            (
                lambda: kikimr.control_plane.get_mkql_allocated(1)
                == (task_count - task_count_0) * kikimr.mkql_initial_memory_limit
            )
        ), "Task memory was not freed"

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.STREAMING).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.RUNNING)

        assert wait_until((lambda: kikimr.control_plane.get_task_count(1, query_id) > 0)), (
            "TaskController not started " + query_id
        )
        task_count += kikimr.control_plane.get_task_count(1, query_id) - task_count_0
        assert wait_until(
            (lambda: kikimr.control_plane.get_mkql_allocated(1) == task_count * kikimr.mkql_initial_memory_limit)
        ), "Task memory was not allocated"
        queries[0] = query_id

        for q in queries:
            client.abort_query(q)
            client.wait_query_status(q, fq.QueryMeta.ABORTED_BY_USER)

        assert wait_until((lambda: kikimr.control_plane.get_mkql_allocated() == 0)), "Incorrect final free"

    @pytest.mark.parametrize("kikimr", [(1 * G, 8 * G, None, None)], indirect=["kikimr"])
    def test_alloc_and_free(self, kikimr):
        kikimr.control_plane.wait_bootstrap(1)
        assert kikimr.control_plane.get_mkql_limit(1) == kikimr.mkql_total_memory_limit, "Incorrect Limit"
        assert kikimr.control_plane.get_mkql_allocated(1) == 0, "Incorrect Alloc"

        self.init_topics("select_alloc_and_free", create_output=False)

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
        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.STREAMING).result.query_id

        client.wait_query_status(query_id, fq.QueryMeta.RUNNING)

        task_count = kikimr.control_plane.get_task_count(1, query_id)
        assert (
            kikimr.control_plane.get_mkql_allocated(1) == task_count * kikimr.mkql_initial_memory_limit
        ), "Incorrect Alloc"

        client.abort_query(query_id)
        client.wait_query_status(query_id, fq.QueryMeta.ABORTED_BY_USER)
        assert kikimr.control_plane.get_mkql_allocated(1) == 0, "Incorrect Alloc"

    @pytest.mark.parametrize("kikimr", [(1 * M, 1 * G, 16 * K, None)], indirect=["kikimr"])
    def test_up_down(self, kikimr):
        kikimr.control_plane.wait_bootstrap(1)
        assert kikimr.control_plane.get_mkql_limit(1) == kikimr.mkql_total_memory_limit, "Incorrect Limit"
        assert kikimr.control_plane.get_mkql_allocated() == 0, "Incorrect Alloc"

        self.init_topics("select_up_down")

        sql = R'''
            INSERT INTO myyds.`{output_topic}`
            SELECT Data
            FROM myyds.`{input_topic}`
            GROUP BY HOP(Just(CurrentUtcTimestamp()), "PT10S", "PT10S", "PT10S"), Data
            '''.format(
            input_topic=self.input_topic,
            output_topic=self.output_topic,
        )

        client = FederatedQueryClient("my_folder", streaming_over_kikimr=kikimr)

        client.create_yds_connection("myyds", os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"))

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.STREAMING).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.RUNNING)
        kikimr.control_plane.wait_zero_checkpoint(query_id)
        assert wait_until((lambda: kikimr.control_plane.get_task_count(1, query_id) > 0)), "TaskController not started"
        allocated_at_start = kikimr.control_plane.get_mkql_allocated(1)

        for i in range(10000):
            self.write_stream([format(i, "09x")])
            allocated = kikimr.control_plane.get_mkql_allocated(1)
            if allocated > allocated_at_start + 1 * M:
                break
        else:
            assert False, "Memory limit was not increased"

        # de-allocation doesn't work as expected
        # assert wait_until((lambda : kikimr.get_mkql_allocated(1) < allocated), timeout=60), "Memory limit was not decreased"

        client.abort_query(query_id)
        client.wait_query_status(query_id, fq.QueryMeta.ABORTED_BY_USER)
        assert kikimr.control_plane.get_mkql_allocated(1) == 0, "Incorrect Alloc"

    @pytest.mark.parametrize("kikimr", [(1 * M, 10 * M, 1 * G, None)], indirect=["kikimr"])
    def test_mkql_not_increased(self, kikimr):
        kikimr.control_plane.wait_bootstrap(1)
        assert kikimr.control_plane.get_mkql_limit(1) == kikimr.mkql_total_memory_limit, "Incorrect Limit"
        assert kikimr.control_plane.get_mkql_allocated() == 0, "Incorrect Alloc"

        self.init_topics("test_mkql_not_increased")

        sql = R'''
            INSERT INTO myyds.`{output_topic}`
            SELECT Data
            FROM myyds.`{input_topic}`
            GROUP BY HOP(Just(CurrentUtcTimestamp()), "PT10S", "PT10S", "PT10S"), Data
            '''.format(
            input_topic=self.input_topic,
            output_topic=self.output_topic,
        )

        client = FederatedQueryClient("my_folder", streaming_over_kikimr=kikimr)

        client.create_yds_connection("myyds", os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"))

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.STREAMING).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.RUNNING)
        kikimr.control_plane.wait_zero_checkpoint(query_id)
        assert wait_until((lambda: kikimr.control_plane.get_task_count(1, query_id) > 0)), "TaskController not started"

        for i in range(10000):
            self.write_stream([format(i, "09x")])
            query = client.describe_query(query_id).result.query
            issues = query.transient_issue
            if len(issues) >= 1:
                assert issues[0].message.startswith(
                    "Mkql memory limit exceeded, limit: 1048576"
                ), "Incorrect message text"
                assert issues[0].message.endswith("canAllocateExtraMemory: 1"), "Incorrect settings"
                assert issues[0].issue_code == 2029, "Incorrect issue code" + issues[0].message
                break
        else:
            assert False, "Memory limit was not reached"

        assert kikimr.control_plane.get_mkql_allocated(1) == 0, "Incorrect Alloc"

    @pytest.mark.parametrize("kikimr", [(350 * K, 100 * M, 1 * K, 400 * K)], indirect=["kikimr"])
    def test_hard_limit(self, kikimr):
        kikimr.control_plane.wait_bootstrap(1)
        assert kikimr.control_plane.get_mkql_limit(1) == kikimr.mkql_total_memory_limit, "Incorrect Limit"
        assert kikimr.control_plane.get_mkql_allocated() == 0, "Incorrect Alloc"

        client = FederatedQueryClient("my_folder@my_cloud", streaming_over_kikimr=kikimr)
        sql = R'''
            SELECT ListLast(ListCollect(ListFromRange(0, {n} + 1)))
            '''
        n = 1
        for i in range(0, 10):
            query_id = client.create_query(
                "simple", sql.format(n=n), type=fq.QueryContent.QueryType.STREAMING
            ).result.query_id
            status = client.wait_query_status(query_id, [fq.QueryMeta.COMPLETED, fq.QueryMeta.FAILED])
            if status == fq.QueryMeta.FAILED:
                assert i > 1, "First queries must be successfull"
                query = client.describe_query(query_id).result.query
                describe_str = str(query)
                assert "LIMIT_EXCEEDED" in describe_str
                break
            n = n * 10
        else:
            assert False, "Limit was NOT exceeded"
