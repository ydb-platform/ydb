#!/usr/bin/env python
# -*- coding: utf-8 -*-

from hamcrest import assert_that

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.clients.kikimr_http_client import HiveClient, SwaggerClient
from ydb.tests.library.kv.helpers import create_kv_tablets_and_wait_for_start
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds

TIMEOUT_SECONDS = 480


class TestChannelsOps(object):
    @classmethod
    def setup_class(cls):
        cls.kikimr_cluster = KiKiMR()
        cls.kikimr_cluster.start()
        cls.client = cls.kikimr_cluster.client
        cls.kv_client = cls.kikimr_cluster.kv_client
        cls.scheme_client = cls.kikimr_cluster.scheme_client
        host = cls.kikimr_cluster.nodes[1].host
        mon_port = cls.kikimr_cluster.nodes[1].mon_port
        cls.hive_client = HiveClient(host, mon_port)
        cls.swagger_client = SwaggerClient(host, mon_port)

    @classmethod
    def teardown_class(cls):
        cls.kikimr_cluster.stop()

    def test_when_write_and_change_tablet_channel_then_can_read_from_tablet(self):
        # Arrange
        number_of_tablets = 10

        path = '/Root/mydb'
        table_path = '/Root/mydb/mytable'
        # table_path_second = '/Root/mydb/mytable2'
        self.scheme_client.make_directory(path)
        tablet_ids = create_kv_tablets_and_wait_for_start(self.client, self.kv_client, self.swagger_client, number_of_tablets, table_path, timeout_seconds=TIMEOUT_SECONDS)

        key = 'key'
        # Act
        for partition_id, tablet_id in enumerate(tablet_ids):
            self.kv_client.kv_write(table_path, partition_id, key, value_for(key, tablet_id))
            self.hive_client.change_tablet_group(tablet_id)

        # Assert
        for partition_id in range(number_of_tablets):
            self.kv_client.kv_read(table_path, partition_id, key)

        for partition_id, tablet_id in enumerate(tablet_ids):
            response = self.kv_client.kv_read(table_path, partition_id, key)
            assert_that(response.operation.status == StatusIds.SUCCESS)
            result = response.operation.result
            assert_that(value_for(key, tablet_id) in result.value.decode("utf-8"))

    def test_when_write_in_new_channel_then_can_read_from_tablet(self):
        # Arrange
        number_of_tablets = 10

        path = '/Root/mydb'
        table_path = '/Root/mydb/mytable'
        self.scheme_client.make_directory(path)
        tablet_ids = create_kv_tablets_and_wait_for_start(self.client, self.kv_client, self.swagger_client, number_of_tablets, table_path, timeout_seconds=TIMEOUT_SECONDS)

        key = 'key'
        key2 = 'key2'
        # Act
        for partition_id, tablet_id in enumerate(tablet_ids):
            self.kv_client.kv_write(table_path, partition_id, key, value_for(key, tablet_id))
            self.hive_client.change_tablet_group(tablet_id)

        for partition_id, tablet_id in enumerate(tablet_ids):
            self.kv_client.kv_write(table_path, partition_id, key2, value_for(key2, tablet_id))

        # Assert
        for partition_id, tablet_id in enumerate(tablet_ids):
            response1 = self.kv_client.kv_read(table_path, partition_id, key)
            response2 = self.kv_client.kv_read(table_path, partition_id, key2)
            assert_that(response1.operation.status == StatusIds.SUCCESS)
            assert_that(response2.operation.status == StatusIds.SUCCESS)
            assert_that(value_for(key, tablet_id) in response1.operation.result.value.decode("utf-8"))
            assert_that(value_for(key2, tablet_id) in response2.operation.result.value.decode("utf-8"))


def value_for(key, tablet_id):
    return "Value: <key = {key}, tablet_id = {tablet_id}>".format(
        key=key, tablet_id=tablet_id)
