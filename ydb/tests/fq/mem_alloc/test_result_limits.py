#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest
import time

from ydb.tests.tools.fq_runner.fq_client import FederatedQueryClient
from ydb.tests.tools.fq_runner.kikimr_runner import StreamingOverKikimr
from ydb.tests.tools.fq_runner.kikimr_runner import StreamingOverKikimrConfig

import ydb.public.api.protos.draft.fq_pb2 as fq


# Quota per cloud
QUOTA_ANALYTICS_COUNT_LIMIT = "yq.analyticsQuery.count"
QUOTA_STREAMING_COUNT_LIMIT = "yq.streamingQuery.count"
QUOTA_CPU_PERCENT_LIMIT = "yq.cpuPercent.count"
QUOTA_MEMORY_LIMIT = "yq.memory.size"
QUOTA_RESULT_LIMIT = "yq.result.size"

# Quota per query
QUOTA_ANALYTICS_DURATION_LIMIT = "yq.analyticsQueryDurationMinutes.count"
QUOTA_STREAMING_DURATION_LIMIT = "yq.streamingQueryDurationMinutes.count"  # internal, for preview purposes
QUOTA_QUERY_RESULT_LIMIT = "yq.queryResult.size"


@pytest.fixture
def kikimr(request):
    kikimr_conf = StreamingOverKikimrConfig(cloud_mode=True)
    kikimr = StreamingOverKikimr(kikimr_conf)
    if hasattr(request, "param"):
        kikimr.control_plane.fq_config['quotas_manager'] = {}
        kikimr.control_plane.fq_config['quotas_manager']['enabled'] = True
        kikimr.control_plane.fq_config['quotas_manager']['quotas'] = []
        for cloud, limit in request.param.items():
            kikimr.control_plane.fq_config['quotas_manager']['quotas'].append(
                {
                    "subject_type": "cloud",
                    "subject_id": cloud,
                    "limit": [{"name": QUOTA_QUERY_RESULT_LIMIT, "limit": limit}],
                }
            )
    kikimr.start_mvp_mock_server()
    kikimr.start()
    yield kikimr
    kikimr.stop_mvp_mock_server()
    kikimr.stop()


def wait_until(predicate, wait_time=10, wait_step=0.5):
    deadline = time.time() + wait_time
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(wait_step)
    else:
        return False


class TestResultLimits(object):
    def test_many_rows(self, kikimr):
        kikimr.control_plane.wait_bootstrap(1)
        assert kikimr.control_plane.get_mkql_allocated(1) == 0, "Incorrect Alloc"

        sql = R'''
SELECT * FROM AS_TABLE(()->(Yql::ToStream(ListReplicate(<|x:
"0123456789ABCDEF"
|>, 4000000000))));
'''
        client = FederatedQueryClient("my_folder@cloud", streaming_over_kikimr=kikimr)

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.STREAMING).result.query_id

        client.wait_query_status(query_id, fq.QueryMeta.FAILED, timeout=600)
        issue = client.describe_query(query_id).result.query.issue[0]
        assert "LIMIT_EXCEEDED" in issue.message, "Incorrect issue " + issue.message
        assert "Can not write results with size > 20971520 byte(s)" in issue.issues[0].message, (
            "Incorrect issue " + issue.issues[0].message
        )

    def test_large_row(self, kikimr):
        kikimr.control_plane.wait_bootstrap(1)
        assert kikimr.control_plane.get_mkql_allocated(1) == 0, "Incorrect Alloc"

        sql = R'''
SELECT ListReplicate("A", 10000000);
'''
        client = FederatedQueryClient("my_folder", streaming_over_kikimr=kikimr)

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.STREAMING).result.query_id

        client.wait_query_status(query_id, fq.QueryMeta.FAILED, timeout=600)
        issue = client.describe_query(query_id).result.query.issue[0]
        assert "LIMIT_EXCEEDED" in issue.message, "Incorrect issue " + issue.message
        assert "Can not write Row[0] with size" in issue.issues[0].message and "(> 10_MB)" in issue.issues[0].message, (
            "Incorrect issue " + issue.issues[0].message
        )

    @pytest.mark.parametrize("kikimr", [{"tiny": 2, "huge": 1000000}], indirect=["kikimr"])
    def test_quotas(self, kikimr):
        huge_client = FederatedQueryClient("my_folder@huge", streaming_over_kikimr=kikimr)
        huge_query_id = huge_client.create_query(
            "simple", "select 1000", type=fq.QueryContent.QueryType.STREAMING
        ).result.query_id
        huge_client.wait_query_status(huge_query_id, fq.QueryMeta.COMPLETED)

        tiny_client = FederatedQueryClient("my_folder@tiny", streaming_over_kikimr=kikimr)
        tiny_query_id = tiny_client.create_query(
            "simple", "select 1000", type=fq.QueryContent.QueryType.STREAMING
        ).result.query_id
        tiny_client.wait_query_status(tiny_query_id, fq.QueryMeta.FAILED)
        issue = tiny_client.describe_query(tiny_query_id).result.query.issue[0]
        assert "LIMIT_EXCEEDED" in issue.message, "Incorrect issue " + issue.message
        assert "Can not write results with size > 2 byte(s)" in issue.issues[0].message, (
            "Incorrect issue " + issue.issues[0].message
        )
