# -*- coding: utf-8 -*-
from hamcrest import assert_that

from ydb.tests.library.common.delayed import wait_tablets_are_active
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
from ydb.tests.library.common.types import Erasure, TabletTypes
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.clients.kikimr_http_client import SwaggerClient

from ydb.tests.library.kv.helpers import get_kv_tablet_ids, create_kv_tablets_and_wait_for_start

TIMEOUT_SECONDS = 240


class TestHive(object):
    @classmethod
    def setup_class(cls):
        configurator = KikimrConfigGenerator(Erasure.BLOCK_4_2, nodes=8)
        cls.cluster = KiKiMR(configurator=configurator)
        cls.cluster.start()
        cls.client = cls.cluster.client
        cls.kv_client = cls.cluster.kv_client
        cls.scheme_client = cls.cluster.scheme_client
        host = cls.cluster.nodes[1].host
        mon_port = cls.cluster.nodes[1].mon_port
        cls.swagger_client = SwaggerClient(host, mon_port)

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, 'cluster'):
            cls.cluster.stop()

    def test_when_create_tablets_after_bs_groups_and_kill_hive_then_tablets_start(self):
        num_of_tablets_in_batch = 5
        path = '/Root/mydb'
        table_path = '/Root/mydb/mytable'
        # Arrange
        self.scheme_client.make_directory(path)
        response = self.kv_client.create_tablets(num_of_tablets_in_batch, table_path)

        assert_that(response.operation.status == StatusIds.SUCCESS)
        tablet_ids = get_kv_tablet_ids(self.swagger_client)

        self.cluster.add_storage_pool(erasure=Erasure.MIRROR_3)
        self.client.tablet_kill(TabletTypes.FLAT_HIVE.tablet_id_for(0))

        wait_tablets_are_active(self.client, tablet_ids)

    def test_when_create_tablets_then_can_lookup_them(self):
        num_of_tablets_to_create = 2
        path = '/Root/mydb'
        table_path = '/Root/mydb/mytable'
        # Arrange
        self.scheme_client.make_directory(path)
        create_kv_tablets_and_wait_for_start(self.client, self.kv_client, self.swagger_client, num_of_tablets_to_create, table_path)
        responses = self.kv_client.kv_get_tablets_write_state(table_path, range(num_of_tablets_to_create))
        assert_that(all(response.operation.status == StatusIds.SUCCESS for response in responses))
