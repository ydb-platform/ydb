# -*- coding: utf-8 -*-
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.stress.s3_workload.workload import WorkloadRunner
from ydb.tests.stress.common.common import YdbClient


class TestYdbWorkload(object):
    @classmethod
    def setup_class(cls):
        config = KikimrConfigGenerator(
            extra_feature_flags={}
        )
        cls.cluster = KiKiMR(config)
        cls.cluster.start()

    @classmethod
    def teardown_class(cls):
        cls.cluster.stop()

    def test(self):
        endpoint = f'grpc://localhost:{self.cluster.nodes[1].grpc_port}'
        client = YdbClient(endpoint, '/Root', True)
        client.wait_connection()
        with WorkloadRunner(client, endpoint, 120) as runner:
            runner.run()
