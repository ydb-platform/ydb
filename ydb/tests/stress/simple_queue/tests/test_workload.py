# -*- coding: utf-8 -*-
import pytest

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.common.types import Erasure
from ydb.tests.stress.simple_queue.workload import Workload


class TestYdbWorkload(object):
    @classmethod
    def setup_class(cls):
        config_generator = KikimrConfigGenerator(erasure=Erasure.MIRROR_3_DC)
        config_generator.yaml_config["table_service_config"]["allow_olap_data_query"] = True
        cls.cluster = KiKiMR(config_generator)
        cls.cluster.start()

    @classmethod
    def teardown_class(cls):
        cls.cluster.stop()

    @pytest.mark.parametrize('mode', ['row', 'column'])
    def test(self, mode: str):
        with Workload(f'grpc://localhost:{self.cluster.nodes[1].grpc_port}', '/Root', 60, mode) as workload:
            for handle in workload.loop():
                handle()
