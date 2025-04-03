# -*- coding: utf-8 -*-
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.stress.oltp_workload.workload import WorkloadRunner
from ydb.tests.stress.common.common import YdbClient


class TestYdbWorkload(object):
    @classmethod
    def setup_class(cls):
        config = KikimrConfigGenerator(
            extra_feature_flags={
                "enable_parameterized_decimal": True,
                "enable_table_datetime64" : True
            }
        )
        cls.cluster = KiKiMR(config)
        cls.cluster.start()

    @classmethod
    def teardown_class(cls):
        cls.cluster.stop()

    def test(self):
        client = YdbClient(f'grpc://localhost:{self.cluster.nodes[1].grpc_port}', '/Root', True)
        client.wait_connection()
        with WorkloadRunner(client, 'oltp_workload', 120) as runner:
            runner.run()
