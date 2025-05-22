#!/usr/bin/env python
# -*- coding: utf-8 -*-

from ydb.tests.tools.fq_runner.kikimr_runner import StreamingOverKikimr
from ydb.tests.tools.fq_runner.kikimr_runner import StreamingOverKikimrConfig
from ydb.tests.tools.fq_runner.kikimr_runner import TenantConfig
from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase


class TestBase(TestYdsBase):
    @classmethod
    def setup_class(cls):
        kikirm_conf = StreamingOverKikimrConfig(
            cloud_mode=True, node_count={"/cp": TenantConfig(1), "/alpha": TenantConfig(1)}
        )
        cls.streaming_over_kikimr = StreamingOverKikimr(kikirm_conf)
        cls.streaming_over_kikimr.control_plane.fq_config['control_plane_storage']['task_lease_ttl'] = "4s"
        cls.streaming_over_kikimr.compute_plane.fq_config['pinger']['ping_period'] = "2s"
        cls.streaming_over_kikimr.start()

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, "streaming_over_kikimr"):
            cls.streaming_over_kikimr.stop()
