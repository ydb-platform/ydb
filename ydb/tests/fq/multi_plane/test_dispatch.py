#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest

from ydb.tests.tools.fq_runner.kikimr_runner import StreamingOverKikimr
from ydb.tests.tools.fq_runner.kikimr_runner import StreamingOverKikimrConfig
from ydb.tests.tools.fq_runner.kikimr_runner import TenantConfig
from ydb.tests.tools.fq_runner.fq_client import FederatedQueryClient

import ydb.public.api.protos.draft.fq_pb2 as fq


@pytest.fixture
def kikimr():
    kikimr_conf = StreamingOverKikimrConfig(
        cloud_mode=True,
        node_count={"/cp": TenantConfig(1), "/alpha": TenantConfig(1), "/beta": TenantConfig(1)},
        tenant_mapping={"alpha": "/alpha", "beta": "/beta"},
        cloud_mapping={"a_cloud": "alpha", "b_cloud": "beta"},
    )
    kikimr = StreamingOverKikimr(kikimr_conf)
    kikimr.start_mvp_mock_server()
    kikimr.start()
    yield kikimr
    kikimr.stop()
    kikimr.stop_mvp_mock_server()


class TestMapping(object):
    def test_mapping(self, kikimr):
        sql = "select 101"
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
