# -*- coding: utf-8 -*-
import pytest

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.common.types import Erasure
from ydb.tests.stress.transfer.workload import Workload


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
        with Workload(f'grpc://localhost:{self.cluster.nodes[1].grpc_port}', '/Root', 60, store_type) as workload:
            workload.loop()
