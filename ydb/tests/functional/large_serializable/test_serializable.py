# -*- coding: utf-8 -*-
import sys

import yatest

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.common.types import Erasure


class Test(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = KiKiMR(KikimrConfigGenerator(erasure=Erasure.MIRROR_3_DC))
        cls.cluster.start()

    @classmethod
    def teardown_class(cls):
        cls.cluster.stop()

    def test(self):
        yatest.common.execute(
            [
                yatest.common.binary_path('ydb/tests/tools/ydb_serializable/ydb_serializable'),
                '--endpoint=localhost:%d' % self.cluster.nodes[1].grpc_port,
                '--database=/Root',
                '--output-path=%s' % yatest.common.output_path(),
                '--iterations=25',
                '--processes=2'
            ],
            stderr=sys.stderr,
            wait=True,
            stdout=sys.stdout,
        )
