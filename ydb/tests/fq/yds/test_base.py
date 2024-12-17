#!/usr/bin/env python
# -*- coding: utf-8 -*-

from ydb.tests.tools.fq_runner.kikimr_runner import StreamingOverKikimr
from ydb.tests.tools.fq_runner.kikimr_runner import StreamingOverKikimrConfig
from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase


class TestBaseWithAbortingConfigParams(TestYdsBase):
    @classmethod
    def setup_class(cls):
        kikimr_conf = StreamingOverKikimrConfig(cloud_mode=True)
        cls.streaming_over_kikimr = StreamingOverKikimr(kikimr_conf)
        cls.streaming_over_kikimr.control_plane.fq_config['control_plane_storage']['task_lease_ttl'] = "2s"
        cls.streaming_over_kikimr.control_plane.fq_config['control_plane_storage']['task_lease_retry_policy'] = {}
        cls.streaming_over_kikimr.control_plane.fq_config['control_plane_storage']['task_lease_retry_policy'][
            'retry_count'
        ] = 1
        cls.streaming_over_kikimr.compute_plane.fq_config['pinger']['ping_period'] = "1s"
        cls.streaming_over_kikimr.start_mvp_mock_server()
        cls.streaming_over_kikimr.start()

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, "streaming_over_kikimr"):
            cls.streaming_over_kikimr.stop_mvp_mock_server()
            cls.streaming_over_kikimr.stop()
