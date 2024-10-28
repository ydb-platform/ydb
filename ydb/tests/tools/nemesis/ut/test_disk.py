# -*- coding: utf-8 -*-
from hamcrest import assert_that, greater_than_or_equal_to

from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.common import types
from ydb.tests.library.common.wait_for import wait_for
from ydb.tests.tools.nemesis.library import disk


class TestSafeDiskBreak(object):

    @classmethod
    def setup_class(cls):
        cls.cluster = kikimr_cluster_factory(
            KikimrConfigGenerator(erasure=types.Erasure.BLOCK_4_2, nodes=9, use_in_memory_pdisks=True))
        cls.cluster.start()

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, 'cluster'):
            cls.cluster.stop()

    def test_erase_method(self):
        nemesis = disk.SafelyBreakDisk(self.cluster)

        def predicate():
            nemesis.prepare_state()
            return nemesis.state_ready

        wait_for(predicate, 180)
        for _ in range(100):
            nemesis.inject_fault()
            if nemesis.successful_data_erase_count >= 1:
                break

        assert_that(
            nemesis.successful_data_erase_count,
            greater_than_or_equal_to(
                1
            )
        )
