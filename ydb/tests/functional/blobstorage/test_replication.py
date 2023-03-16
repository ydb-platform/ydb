#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pytest
from hamcrest import is_

from ydb.tests.library.common.types import Erasure
from ydb.tests.library.common.wait_for import wait_for_and_assert
from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.predicates import blobstorage


TIMEOUT_SECONDS = 480


class TestReplicationAfterNodesRestart(object):
    def __setup_cluster(self, erasure):
        self.cluster = kikimr_cluster_factory(configurator=KikimrConfigGenerator(erasure=erasure))
        self.cluster.start()

    def teardown_method(self, method=None):
        if hasattr(self, 'cluster'):
            self.cluster.stop()

    @pytest.mark.parametrize('erasure', Erasure.common_used(), ids=str)
    def test_replication(self, erasure):
        # Arrange
        self.__setup_cluster(erasure)

        # Act
        for node in self.cluster.nodes.values():
            node.stop()
            node.start()

        wait_for_and_assert(
            lambda: blobstorage.cluster_has_no_unsynced_vdisks(self.cluster),
            is_(True), timeout_seconds=TIMEOUT_SECONDS, message='All vdisks are sync'
        )
