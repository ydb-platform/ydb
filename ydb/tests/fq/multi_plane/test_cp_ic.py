#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest

from ydb.tests.tools.fq_runner.kikimr_runner import StreamingOverKikimr
from ydb.tests.tools.fq_runner.kikimr_runner import StreamingOverKikimrConfig


@pytest.fixture
def kikimr():
    kikimr_conf = StreamingOverKikimrConfig(cloud_mode=True)
    kikimr = StreamingOverKikimr(kikimr_conf)
    kikimr.control_plane.fq_config['control_plane_storage']['mapping'] = {"common_tenant_name": ["/alpha"]}
    kikimr.control_plane.fq_config['private_api']['loopback'] = True
    kikimr.control_plane.fq_config['nodes_manager']['enabled'] = True
    kikimr.start_mvp_mock_server()
    kikimr.start()
    yield kikimr
    kikimr.stop()
    kikimr.stop_mvp_mock_server()


class TestCpIc(object):
    def test_discovery(self, kikimr):
        kikimr.control_plane.wait_discovery()
