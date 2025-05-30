# -*- coding: utf-8 -*-
import os
import pytest
import yatest

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.common.types import Erasure


class TestYdbTransferWorkload(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = KiKiMR(KikimrConfigGenerator(
            erasure=Erasure.MIRROR_3_DC,
            extra_feature_flags={
                "enable_topic_transfer": True,
            }
        ))
        cls.cluster.start()

    @classmethod
    def teardown_class(cls):
        cls.cluster.stop()

    @pytest.mark.parametrize("store_type", ["row", "column"])
    def test(self, store_type):
        cmd = [
            yatest.common.binary_path(os.getenv("YDB_TEST_PATH")),
            "--endpoint", f'grpc://localhost:{self.cluster.nodes[1].grpc_port}',
            "--database", "/Root",
            "--duration", "60",
            "--mode", store_type
        ]
        yatest.common.execute(cmd, wait=True)
