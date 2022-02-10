#!/usr/bin/env python
# -*- coding: utf-8 -*-
from hamcrest import is_

from ydb.tests.library.common.wait_for import wait_for_and_assert
from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from ydb.tests.library.predicates import blobstorage


TIMEOUT_SECONDS = 480


class TestEnableSelfHeal(object):
    @classmethod
    def setup_class(cls):
        cls.kikimr_cluster = kikimr_cluster_factory()
        cls.kikimr_cluster.start()
        cls.client = cls.kikimr_cluster.client
        cls.client.update_self_heal(True)

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, 'kikimr_cluster'):
            cls.kikimr_cluster.stop()

    def test_replication(self):
        for node in self.kikimr_cluster.nodes.values():
            node.stop()
            node.start()

        # TODO break pdisks and wait for self heal to repair cluster

        wait_for_and_assert(
            lambda: blobstorage.cluster_has_no_unsynced_vdisks(self.kikimr_cluster),
            is_(True), timeout_seconds=TIMEOUT_SECONDS, message='All vdisks are sync'
        )
