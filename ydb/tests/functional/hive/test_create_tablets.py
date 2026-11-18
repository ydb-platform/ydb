# -*- coding: utf-8 -*-
import json
from urllib.request import urlopen
from ydb.public.api.protos import ydb_keyvalue_pb2
from hamcrest import assert_that

from ydb.tests.library.common.delayed import wait_tablets_are_active
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
from ydb.tests.library.common.types import Erasure, TabletTypes
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.clients.kikimr_http_client import SwaggerClient

from ydb.tests.library.kv.helpers import get_kv_volume_tablet_ids, create_kv_tablets_and_wait_for_start

TIMEOUT_SECONDS = 240


class TestHive(object):
    erasure = Erasure.BLOCK_4_2
    value = b"persistent block erasure data" * 4096

    @classmethod
    def setup_class(cls):
        configurator = KikimrConfigGenerator(cls.erasure, nodes=cls.erasure.min_fail_domains)
        cls.cluster = KiKiMR(configurator=configurator)
        cls.cluster.start()
        host = cls.cluster.nodes[1].host
        mon_port = cls.cluster.nodes[1].mon_port
        cls.swagger_client = SwaggerClient(host, mon_port)

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, 'cluster'):
            cls.cluster.stop()

    def tablet_info(self, tablet_id):
        node = self.cluster.nodes[1]
        url = (f'http://{node.host}:{node.mon_port}/tablets/app?'
               f'TabletID={TabletTypes.FLAT_HIVE.tablet_id_for(0)}&page=TabletInfo&tablet={tablet_id}')
        with urlopen(url, timeout=10) as response:
            return json.load(response)

    def check_value(self, table_path):
        response = self.cluster.kv_client.kv_read(table_path, 0, 'persistent-key')
        assert response.operation.status == StatusIds.SUCCESS
        result = ydb_keyvalue_pb2.ReadResult()
        assert response.operation.result.Unpack(result)
        assert result.value == self.value

    def test_when_create_tablets_after_bs_groups_and_kill_hive_then_tablets_start(self):
        def channel_history(channels):
            return [(channel['Channel'], [(entry['FromGeneration'], entry['GroupID'])
                                          for entry in channel['History']]) for channel in channels]

        num_of_tablets_in_batch = 5
        path = '/Root/mydb'
        table_path = '/Root/mydb/mytable'
        # Arrange
        self.cluster.scheme_client.make_directory(path)
        response = self.cluster.kv_client.create_tablets(num_of_tablets_in_batch, table_path)

        assert_that(response.operation.status == StatusIds.SUCCESS)
        tablet_ids = get_kv_volume_tablet_ids(self.cluster.client, table_path, num_of_tablets_in_batch)

        response = self.cluster.kv_client.kv_write(table_path, 0, 'persistent-key', self.value)
        assert response.operation.status == StatusIds.SUCCESS
        channels = self.tablet_info(tablet_ids[0])['TabletStorageInfo']['Channels']
        history = channel_history(channels)
        groups = {group.GroupId: group.ErasureSpecies for group in self.cluster.client.query_base_config().BaseConfig.Group}
        assert channels and all(channel['History'] for channel in channels)
        assert all(groups[int(entry['GroupID'])] == str(self.erasure)
                   for channel in channels for entry in channel['History'])
        self.cluster.add_storage_pool(erasure=self.erasure)
        self.cluster.client.tablet_kill(TabletTypes.FLAT_HIVE.tablet_id_for(0))

        wait_tablets_are_active(self.cluster.client, tablet_ids)
        self.check_value(table_path)
        info = self.tablet_info(tablet_ids[0])
        assert channel_history(info['TabletStorageInfo']['Channels']) == history
        node = self.cluster.nodes[int(info['NodeId'])]
        node.stop()
        node.start()
        wait_tablets_are_active(self.cluster.client, tablet_ids)
        self.check_value(table_path)
        assert channel_history(self.tablet_info(tablet_ids[0])['TabletStorageInfo']['Channels']) == history

    def test_when_create_tablets_then_can_lookup_them(self):
        num_of_tablets_to_create = 2
        path = '/Root/mydb'
        table_path = '/Root/mydb/mytable'
        # Arrange
        self.cluster.scheme_client.make_directory(path)
        create_kv_tablets_and_wait_for_start(self.cluster.client, self.cluster.kv_client, self.swagger_client, num_of_tablets_to_create, table_path)
        responses = self.cluster.kv_client.kv_get_tablets_write_state(table_path, range(num_of_tablets_to_create))
        assert_that(all(response.operation.status == StatusIds.SUCCESS for response in responses))


class TestHiveBlock82(TestHive):
    erasure = Erasure.BLOCK_8_2
