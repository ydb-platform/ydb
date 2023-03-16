#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging

from hamcrest import assert_that

from ydb.tests.library.common.protobuf import KVRequest
from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from ydb.tests.library.harness.kikimr_http_client import HiveClient
from ydb.tests.library.kv.helpers import create_tablets_and_wait_for_start
from ydb.tests.library.matchers.response import is_valid_keyvalue_protobuf_response


TIMEOUT_SECONDS = 480
logger = logging.getLogger()


class TestChannelsOps(object):
    @classmethod
    def setup_class(cls):
        cls.kikimr_cluster = kikimr_cluster_factory()
        cls.kikimr_cluster.start()
        cls.client = cls.kikimr_cluster.client
        host = cls.kikimr_cluster.nodes[1].host
        mon_port = cls.kikimr_cluster.nodes[1].mon_port
        cls.hive_client = HiveClient(host, mon_port)

    @classmethod
    def teardown_class(cls):
        cls.kikimr_cluster.stop()

    def test_when_write_and_change_tablet_channel_then_can_read_from_tablet(self):
        # Arrange
        number_of_tablets = 10

        tablet_ids = create_tablets_and_wait_for_start(
            self.client, number_of_tablets,
            batch_size=number_of_tablets,
            timeout_seconds=TIMEOUT_SECONDS
        )

        key = 'key'
        # Act
        for tablet_id in tablet_ids:
            self.client.kv_request(
                tablet_id, KVRequest().write(bytes(key, 'utf-8'), bytes(value_for(key, tablet_id), 'utf-8'))
            )
            self.hive_client.change_tablet_group(tablet_id)

        create_tablets_and_wait_for_start(
            self.client, number_of_tablets,
            batch_size=number_of_tablets,
            timeout_seconds=TIMEOUT_SECONDS
        )

        # Assert
        for tablet_id in tablet_ids:
            self.client.kv_request(tablet_id, KVRequest().read(bytes(key, 'utf-8')))

        for tablet_id in tablet_ids:
            response = self.client.kv_request(
                tablet_id, KVRequest().read(bytes(key, 'utf-8')))
            assert_that(
                response,
                is_valid_keyvalue_protobuf_response().read_key(
                    bytes(value_for(key, tablet_id), 'utf-8'))
            )

    def test_when_write_in_new_channel_then_can_read_from_tablet(self):
        # Arrange
        number_of_tablets = 10

        tablet_ids = create_tablets_and_wait_for_start(
            self.client, number_of_tablets,
            batch_size=number_of_tablets,
            timeout_seconds=TIMEOUT_SECONDS
        )

        # Gather current generations for created tablets
        tablet_generations = {
            state.TabletId: state.Generation
            for state in self.client.tablet_state(tablet_ids=tablet_ids).TabletStateInfo
        }

        key = 'key'
        key2 = 'key2'
        # Act
        for tablet_id in tablet_ids:
            self.client.kv_request(
                tablet_id, KVRequest().write(bytes(key, 'utf-8'), bytes(value_for(key, tablet_id), 'utf-8'))
            )
            self.hive_client.change_tablet_group(tablet_id)

        create_tablets_and_wait_for_start(
            self.client, number_of_tablets,
            batch_size=number_of_tablets,
            timeout_seconds=TIMEOUT_SECONDS,
            skip_generations=tablet_generations,
        )

        for tablet_id in tablet_ids:
            self.client.kv_request(
                tablet_id, KVRequest().write(
                    bytes(key2, 'utf-8'), bytes(value_for(key2, tablet_id), 'utf-8')
                )
            )

        # Assert
        for tablet_id in tablet_ids:
            response = self.client.kv_request(
                tablet_id, KVRequest().read(bytes(key, 'utf-8')).read(bytes(key2, 'utf-8'))
            )
            assert_that(
                response,
                is_valid_keyvalue_protobuf_response().read_key(
                    bytes(value_for(key, tablet_id), 'utf-8')).read_key(
                    bytes(value_for(key2, tablet_id), 'utf-8'))
            )


def value_for(key, tablet_id):
    return "Value: <key = {key}, tablet_id = {tablet_id}>".format(
        key=key, tablet_id=tablet_id)
