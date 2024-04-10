# -*- coding: utf-8 -*-
import sys

from ydb.tests.library.common import yatest_common
from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.common.types import Erasure


class Test(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = kikimr_cluster_factory(KikimrConfigGenerator(erasure=Erasure.MIRROR_3_DC))
        cls.cluster.start()

    @classmethod
    def teardown_class(cls):
        cls.cluster.stop()

    def test(self):
        yatest_common.execute(
            [
                yatest_common.binary_path('ydb/tests/tools/ydb_serializable/ydb_serializable'),
                '--endpoint=localhost:%d' % self.cluster.nodes[1].grpc_port,
                '--database=/Root',
                '--output-path=%s' % yatest_common.output_path(),
                '--iterations=25',
                '--processes=2'
            ],
            stderr=sys.stderr,
            wait=True,
            stdout=sys.stdout,
        )
