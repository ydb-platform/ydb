#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import pytest
import time

from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase
from ydb.tests.tools.fq_runner.kikimr_runner import StreamingOverKikimr
from ydb.tests.tools.fq_runner.kikimr_runner import StreamingOverKikimrConfig
from ydb.tests.tools.fq_runner.fq_client import FederatedQueryClient

import ydb.public.api.protos.draft.fq_pb2 as fq


class Param(object):
    def __init__(
        self,
        retry_limit=2,
        retry_period=20,
        task_lease_ttl=4,
        ping_period=2,
    ):
        self.retry_limit = retry_limit
        self.retry_period = retry_period
        self.task_lease_ttl = task_lease_ttl
        self.ping_period = ping_period


@pytest.fixture
def kikimr(request):
    kikimr_conf = StreamingOverKikimrConfig(cloud_mode=True)
    kikimr = StreamingOverKikimr(kikimr_conf)
    # control
    kikimr.control_plane.fq_config['control_plane_storage']['mapping'] = {"common_tenant_name": ["/compute"]}
    kikimr.control_plane.fq_config['control_plane_storage']['task_lease_retry_policy'] = {}
    kikimr.control_plane.fq_config['control_plane_storage']['task_lease_retry_policy'][
        'retry_count'
    ] = request.param.retry_limit
    kikimr.control_plane.fq_config['control_plane_storage']['task_lease_retry_policy']['retry_period'] = "{}s".format(
        request.param.retry_period
    )
    kikimr.control_plane.fq_config['control_plane_storage']['task_lease_ttl'] = "{}s".format(
        request.param.task_lease_ttl
    )
    # compute
    kikimr.compute_plane.fq_config['pinger']['ping_period'] = "{}s".format(request.param.ping_period)
    kikimr.start_mvp_mock_server()
    kikimr.start()
    yield kikimr
    kikimr.stop()
    kikimr.stop_mvp_mock_server()


class TestRetry(TestYdsBase):
    # TODO: fix this place. We need to speed up the drain and use retry_limit=3, retry_period=20
    @pytest.mark.parametrize(
        "kikimr", [Param(retry_limit=2, retry_period=600, task_lease_ttl=1, ping_period=0.5)], indirect=["kikimr"]
    )
    def test_high_rate(self, kikimr):
        topic_name = "high_rate"
        connection = "high_rate"
        self.init_topics(topic_name)
        sql = R'''SELECT * FROM {connection}.`{input_topic}`;'''.format(
            input_topic=self.input_topic, connection=connection
        )
        client = FederatedQueryClient("my_folder", streaming_over_kikimr=kikimr)
        client.create_yds_connection(connection, os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"))
        query_id = client.create_query("a", sql, type=fq.QueryContent.QueryType.STREAMING).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.RUNNING)
        for _ in range(10):
            deadline = time.time() + 1
            kikimr.compute_plane.stop()
            kikimr.compute_plane.start()
            kikimr.compute_plane.wait_bootstrap()
            if client.describe_query(query_id).result.query.meta.status == fq.QueryMeta.ABORTED_BY_SYSTEM:
                break
            delta = deadline - time.time()
            if delta > 0:
                time.sleep(delta)
        else:
            assert False, "Query was NOT aborted"
