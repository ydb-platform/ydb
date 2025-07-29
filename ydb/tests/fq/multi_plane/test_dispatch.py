#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest
import os
import time

from ydb.tests.tools.fq_runner.kikimr_runner import StreamingOverKikimr
from ydb.tests.tools.fq_runner.kikimr_runner import StreamingOverKikimrConfig
from ydb.tests.tools.fq_runner.kikimr_runner import TenantConfig
from ydb.tests.tools.fq_runner.fq_client import FederatedQueryClient
from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase

import ydb.public.api.protos.draft.fq_pb2 as fq


@pytest.fixture
def kikimr():
    kikimr_conf = StreamingOverKikimrConfig(
        cloud_mode=True,
        node_count={"/cp": TenantConfig(1), "/alpha": TenantConfig(1), "/beta": TenantConfig(2)},
        tenant_mapping={"alpha": "/alpha", "beta": "/beta"},
        cloud_mapping={"a_cloud": ("alpha", None), "b_cloud": ("beta", "1 "), "c_cloud": ("beta", " 2"), "d_cloud": ("beta", " 1 - 2 ")},
    )
    kikimr = StreamingOverKikimr(kikimr_conf)
    kikimr.start_mvp_mock_server()
    kikimr.start()
    yield kikimr
    kikimr.stop()
    kikimr.stop_mvp_mock_server()


class TestMapping(TestYdsBase):
    def test_mapping(self, kikimr):
        sql = R'''
            select 101;
        '''
        a_client = FederatedQueryClient("a_folder@a_cloud", streaming_over_kikimr=kikimr)
        b_client = FederatedQueryClient("b_folder@b_cloud", streaming_over_kikimr=kikimr)
        a_query_id = a_client.create_query("a", sql, type=fq.QueryContent.QueryType.STREAMING).result.query_id
        a_client.wait_query_status(a_query_id, fq.QueryMeta.COMPLETED)
        kikimr.tenants["/alpha"].stop()
        a_query_id = a_client.create_query("a", sql, type=fq.QueryContent.QueryType.STREAMING).result.query_id
        for _ in range(10):
            b_query_id = b_client.create_query("b", sql, type=fq.QueryContent.QueryType.STREAMING).result.query_id
            b_client.wait_query_status(b_query_id, fq.QueryMeta.COMPLETED)
        assert a_client.describe_query(a_query_id).result.query.meta.status == fq.QueryMeta.STARTING
        kikimr.tenants["/alpha"].start()
        a_client.wait_query_status(a_query_id, fq.QueryMeta.COMPLETED)

    def test_idle(self, kikimr):
        sql = "select 107"
        a_client = FederatedQueryClient("a_folder@a_cloud", streaming_over_kikimr=kikimr)
        b_client = FederatedQueryClient("b_folder@b_cloud", streaming_over_kikimr=kikimr)
        a_query_id = a_client.create_query("a", sql, type=fq.QueryContent.QueryType.STREAMING).result.query_id
        a_client.wait_query_status(a_query_id, fq.QueryMeta.COMPLETED)
        kikimr.tenants["/alpha"].stop()
        a_query_id = a_client.create_query("a", sql, type=fq.QueryContent.QueryType.STREAMING).result.query_id
        for _ in range(10):
            b_query_id = b_client.create_query("b", sql, type=fq.QueryContent.QueryType.STREAMING).result.query_id
            b_client.wait_query_status(b_query_id, fq.QueryMeta.COMPLETED)
        assert a_client.describe_query(a_query_id).result.query.meta.status == fq.QueryMeta.STARTING
        kikimr.exec_db_statement(
            """--!syntax_v1
            PRAGMA TablePathPrefix("{}");
            UPDATE mappings SET vtenant = "beta" WHERE subject_id = "a_cloud";
            UPDATE tenants SET state = 2, state_time = CurrentUtcTimestamp() WHERE tenant = "/alpha";
            """
        )
        a_client.wait_query_status(a_query_id, fq.QueryMeta.COMPLETED)

    def test_mapping_with_nodes(self, kikimr):
        self.init_topics("test_mapping_with_nodes", create_output=False)
        sql = R'''SELECT Data FROM yds.`{input_topic}` LIMIT 1'''.format(input_topic=self.input_topic)

        a_client = FederatedQueryClient("a_folder@a_cloud", streaming_over_kikimr=kikimr)
        a_client.create_yds_connection("yds", os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"))
        query_id = a_client.create_query("a", sql, type=fq.QueryContent.QueryType.STREAMING).result.query_id
        kikimr.tenants["/alpha"].wait_completed_checkpoints(query_id, kikimr.tenants["/alpha"].get_completed_checkpoints(query_id) + 2)
        assert self.wait_until((lambda: kikimr.tenants["/alpha"].get_actor_count(1, "YQ_RUN_ACTOR") == 1))
        assert self.wait_until((lambda: kikimr.tenants["/alpha"].get_actor_count(1, "DQ_COMPUTE_ACTOR") > 0))
        a_client.abort_query(query_id)
        a_client.wait_query(query_id)

        b_client = FederatedQueryClient("b_folder@b_cloud", streaming_over_kikimr=kikimr)
        b_client.create_yds_connection("yds", os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"))
        query_id = b_client.create_query("a", sql, type=fq.QueryContent.QueryType.STREAMING).result.query_id
        kikimr.tenants["/beta"].wait_completed_checkpoints(query_id, kikimr.tenants["/beta"].get_completed_checkpoints(query_id) + 2)
        assert self.wait_until((lambda: kikimr.tenants["/beta"].get_actor_count(1, "YQ_RUN_ACTOR") == 1))
        assert self.wait_until((lambda: kikimr.tenants["/beta"].get_actor_count(1, "DQ_COMPUTE_ACTOR") > 0))
        assert self.wait_until((lambda: kikimr.tenants["/beta"].get_actor_count(2, "DQ_COMPUTE_ACTOR") == 0))
        b_client.abort_query(query_id)
        b_client.wait_query(query_id)

        c_client = FederatedQueryClient("c_folder@c_cloud", streaming_over_kikimr=kikimr)
        c_client.create_yds_connection("yds", os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"))
        query_id = c_client.create_query("a", sql, type=fq.QueryContent.QueryType.STREAMING).result.query_id
        kikimr.tenants["/beta"].wait_completed_checkpoints(query_id, kikimr.tenants["/beta"].get_completed_checkpoints(query_id) + 2)
        assert self.wait_until((lambda: kikimr.tenants["/beta"].get_actor_count(2, "YQ_RUN_ACTOR") == 1))
        assert self.wait_until((lambda: kikimr.tenants["/beta"].get_actor_count(2, "DQ_COMPUTE_ACTOR") > 0))
        assert self.wait_until((lambda: kikimr.tenants["/beta"].get_actor_count(1, "DQ_COMPUTE_ACTOR") == 0))
        c_client.abort_query(query_id)
        c_client.wait_query(query_id)

        d_client = FederatedQueryClient("d_folder@d_cloud", streaming_over_kikimr=kikimr)
        d_client.create_yds_connection("yds", os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"))
        query_id = d_client.create_query("a", sql, type=fq.QueryContent.QueryType.STREAMING).result.query_id
        kikimr.tenants["/beta"].wait_completed_checkpoints(query_id, kikimr.tenants["/beta"].get_completed_checkpoints(query_id) + 2)
        assert self.wait_until(lambda: kikimr.tenants["/beta"].get_actor_count(1, "YQ_RUN_ACTOR") == 1 or kikimr.tenants["/beta"].get_actor_count(2, "YQ_RUN_ACTOR") == 1)
        d_client.abort_query(query_id)
        d_client.wait_query(query_id)

    def test_mapping_with_nodes_with_scheduler(self, kikimr):
        self.init_topics("test_mapping_with_nodes_with_scheduler", create_output=False)
        sql = R'''
            pragma dq.Scheduler=@@{{"type": "single_node"}}@@;
            SELECT Data FROM yds.`{input_topic}` LIMIT 1'''.format(input_topic=self.input_topic)
        time.sleep(1)
        a_client = FederatedQueryClient("a_folder@a_cloud", streaming_over_kikimr=kikimr)
        a_client.create_yds_connection("yds", os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"))
        query_id = a_client.create_query("a", sql, type=fq.QueryContent.QueryType.STREAMING).result.query_id
        kikimr.tenants["/alpha"].wait_completed_checkpoints(query_id, kikimr.tenants["/alpha"].get_completed_checkpoints(query_id) + 2)
        assert self.wait_until((lambda: kikimr.tenants["/alpha"].get_actor_count(1, "YQ_RUN_ACTOR") == 1))
        assert self.wait_until((lambda: kikimr.tenants["/alpha"].get_actor_count(1, "DQ_COMPUTE_ACTOR") > 0))
        a_client.abort_query(query_id)
        a_client.wait_query(query_id)

        b_client = FederatedQueryClient("b_folder@b_cloud", streaming_over_kikimr=kikimr)
        b_client.create_yds_connection("yds", os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"))
        query_id = b_client.create_query("a", sql, type=fq.QueryContent.QueryType.STREAMING).result.query_id
        kikimr.tenants["/beta"].wait_completed_checkpoints(query_id, kikimr.tenants["/beta"].get_completed_checkpoints(query_id) + 2)
        assert self.wait_until((lambda: kikimr.tenants["/beta"].get_actor_count(1, "YQ_RUN_ACTOR") == 1))
        assert self.wait_until((lambda: kikimr.tenants["/beta"].get_actor_count(1, "DQ_COMPUTE_ACTOR") > 0))
        assert self.wait_until((lambda: kikimr.tenants["/beta"].get_actor_count(2, "DQ_COMPUTE_ACTOR") == 0))
        b_client.abort_query(query_id)
        b_client.wait_query(query_id)

        c_client = FederatedQueryClient("c_folder@c_cloud", streaming_over_kikimr=kikimr)
        c_client.create_yds_connection("yds", os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"))
        query_id = c_client.create_query("a", sql, type=fq.QueryContent.QueryType.STREAMING).result.query_id
        kikimr.tenants["/beta"].wait_completed_checkpoints(query_id, kikimr.tenants["/beta"].get_completed_checkpoints(query_id) + 2)
        assert self.wait_until((lambda: kikimr.tenants["/beta"].get_actor_count(2, "YQ_RUN_ACTOR") == 1))
        assert self.wait_until((lambda: kikimr.tenants["/beta"].get_actor_count(2, "DQ_COMPUTE_ACTOR") > 0))
        assert self.wait_until((lambda: kikimr.tenants["/beta"].get_actor_count(1, "DQ_COMPUTE_ACTOR") == 0))
        c_client.abort_query(query_id)
        c_client.wait_query(query_id)
