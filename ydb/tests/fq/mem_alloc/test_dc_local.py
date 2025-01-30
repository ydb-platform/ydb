#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os

import pytest

from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase
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
    kikimr_conf = StreamingOverKikimrConfig(
        cloud_mode=True, node_count=4, dc_mapping={1: "DC1", 2: "DC2", 3: "DC1", 4: "DC2"}
    )
    kikimr = StreamingOverKikimr(kikimr_conf)
    kikimr.start_mvp_mock_server()
    kikimr.start()
    yield kikimr
    kikimr.stop_mvp_mock_server()
    kikimr.stop()


class TestAlloc(TestYdsBase):
    @pytest.mark.parametrize("kikimr", [(None, None, None)], indirect=["kikimr"])
    def test_dc_locality(self, kikimr):
        self.init_topics("select_dc_locality", create_output=False)

        sql = R'''
            SELECT * FROM myyds.`{input_topic}`
            '''.format(
            input_topic=self.input_topic,
        )

        client = FederatedQueryClient("my_folder", streaming_over_kikimr=kikimr)

        client.create_yds_connection("myyds", os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"))

        kikimr.control_plane.wait_discovery()

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.STREAMING).result.query_id

        client.wait_query_status(query_id, fq.QueryMeta.RUNNING)
        kikimr.control_plane.wait_zero_checkpoint(query_id)

        w1 = kikimr.control_plane.get_worker_count(1)
        w2 = kikimr.control_plane.get_worker_count(2)
        w3 = kikimr.control_plane.get_worker_count(3)
        w4 = kikimr.control_plane.get_worker_count(4)

        assert ((w1 * w3) != 0) ^ ((w2 * w4) != 0), "Incorrect placement " + str([w1, w2, w3, w4])

        client.abort_query(query_id)
        client.wait_query_status(query_id, fq.QueryMeta.ABORTED_BY_USER)
