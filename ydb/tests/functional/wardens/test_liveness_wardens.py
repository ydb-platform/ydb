#!/usr/bin/env python
# -*- coding: utf-8 -*-
import ydb

from hamcrest import is_, empty, has_length, greater_than_or_equal_to

from ydb.tests.library.wardens.factories import hive_liveness_warden_factory, transactions_processing_liveness_warden
from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from ydb.tests.library.harness.kikimr_http_client import HiveClient
from ydb.tests.library.common.wait_for import wait_for_and_assert


TIMEOUT_SECONDS = 480


class TestLivenessWarden(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = kikimr_cluster_factory()
        cls.cluster.start()

        cls.hive = HiveClient(cls.cluster.nodes[1].host, cls.cluster.nodes[1].mon_port)
        cls.driver_config = ydb.DriverConfig("%s:%s" % (cls.cluster.nodes[1].host, cls.cluster.nodes[1].grpc_port), '/Root')
        cls.driver = ydb.Driver(cls.driver_config)
        cls.driver.wait()

    @classmethod
    def teardown_class(cls):
        cls.driver.stop()
        cls.cluster.stop()

    def test_hive_liveness_warden_reports_issues(self):
        warden = hive_liveness_warden_factory(self.cluster)
        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())
        session.execute_scheme('create table test_hive_liveness_warden_reports_issues (id Int32, primary key(id)); ')

        wait_for_and_assert(
            lambda: warden.list_of_liveness_violations,
            is_(empty()),
            timeout_seconds=TIMEOUT_SECONDS,
        )

        # block all nodes to make all tablets dead
        for node_id in self.cluster.nodes.keys():
            self.hive.block_node(node_id)
            self.hive.kick_tablets_from_node(node_id)

        wait_for_and_assert(
            lambda: warden.list_of_liveness_violations,
            has_length(greater_than_or_equal_to(1)),
            timeout_seconds=TIMEOUT_SECONDS,
        )

        for node_id in self.cluster.nodes.keys():
            self.hive.unblock_node(node_id)

    def test_scheme_shard_has_no_in_flight_transactions(self):
        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())
        session.execute_scheme('create table test_scheme_shard_has_no_in_flight_transactions (id Int32, primary key(id)); ')

        warden = transactions_processing_liveness_warden(self.cluster)
        wait_for_and_assert(
            lambda: warden.list_of_liveness_violations,
            is_(empty()),
            timeout_seconds=TIMEOUT_SECONDS
        )
