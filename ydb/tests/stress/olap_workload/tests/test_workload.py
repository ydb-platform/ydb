# -*- coding: utf-8 -*-
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.common.types import Erasure
from ydb.tests.stress.common.common import YdbClient
from ydb.tests.stress.olap_workload.workload import WorkloadRunner


class TestYdbWorkload(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = KiKiMR(KikimrConfigGenerator(
            erasure=Erasure.MIRROR_3_DC,
            column_shard_config={
                "allow_nullable_columns_in_pk": True,
            }
        ))
        cls.cluster.start()

    @classmethod
    def teardown_class(cls):
        cls.cluster.stop()

    def test(self):
        client = YdbClient(f'grpc://localhost:{self.cluster.nodes[1].grpc_port}', '/Root', True)
        client.wait_connection()
        with WorkloadRunner(client, 'olap_workload', 120, True) as runner:
            runner.run()
