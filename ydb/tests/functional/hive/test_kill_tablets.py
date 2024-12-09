# -*- coding: utf-8 -*-
from hamcrest import assert_that, greater_than, has_length

from ydb.tests.library.common.delayed import wait_tablets_state_by_id
from ydb.tests.library.common.types import TabletTypes, TabletStates
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.clients.kikimr_http_client import SwaggerClient
from ydb.tests.library.matchers.response import is_valid_response_with_field
from ydb.tests.library.kv.helpers import create_kv_tablets_and_wait_for_start

TIMEOUT_SECONDS = 180
NUMBER_OF_TABLETS = 4


class TestKillTablets(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = KiKiMR()
        cls.cluster.start()
        host = cls.cluster.nodes[1].host
        mon_port = cls.cluster.nodes[1].mon_port
        cls.swagger_client = SwaggerClient(host, mon_port)
        cls.client = cls.cluster.client
        cls.kv_client = cls.cluster.kv_client
        cls.scheme_client = cls.cluster.scheme_client

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, 'cluster'):
            cls.cluster.stop()

    def test_when_kill_keyvalue_tablet_it_will_be_restarted(self):
        # Arrange
        path = '/Root/mydb'
        table_path = '/Root/mydb/mytable'
        self.scheme_client.make_directory(path)
        all_created_tablet_ids = create_kv_tablets_and_wait_for_start(self.client, self.kv_client, self.swagger_client, NUMBER_OF_TABLETS, table_path)
        actual_tablet_info = self.client.tablet_state(tablet_ids=all_created_tablet_ids).TabletStateInfo

        # Act
        for tablet_id in all_created_tablet_ids:
            self.client.tablet_kill(tablet_id)

        generations = {info.TabletId: info.Generation for info in actual_tablet_info}
        wait_tablets_state_by_id(
            self.client,
            TabletStates.Active,
            tablet_ids=all_created_tablet_ids,
            skip_generations=generations,
            generation_matcher=greater_than,
            timeout_seconds=TIMEOUT_SECONDS,
        )

    def test_when_kill_hive_it_will_be_restarted_and_can_create_tablets(self):
        # Arrange
        hive_state_response = self.client.tablet_state(tablet_type=TabletTypes.FLAT_HIVE)
        assert_that(
            hive_state_response,
            is_valid_response_with_field('TabletStateInfo', has_length(1))
        )
        hive_id = hive_state_response.TabletStateInfo[0].TabletId

        # Act
        self.client.tablet_kill(hive_id, assert_success=True)

        wait_tablets_state_by_id(
            self.client,
            TabletStates.Active,
            tablet_ids=[hive_id],
            message='Hive is Active'
        )

        path = '/Root/mydb'
        table_path = '/Root/mydb/mytable'
        self.scheme_client.make_directory(path)
        all_tablet_ids = create_kv_tablets_and_wait_for_start(self.client, self.kv_client, self.swagger_client, NUMBER_OF_TABLETS, table_path)

        # Assert
        wait_tablets_state_by_id(
            self.client,
            TabletStates.Active,
            tablet_ids=all_tablet_ids,
            message='New tablets are created',
            timeout_seconds=TIMEOUT_SECONDS,
        )

    def test_then_kill_system_tablets_and_it_increases_generation(self):
        # Arrange
        tablet_types = [
            TabletTypes.FLAT_TX_COORDINATOR, TabletTypes.TX_MEDIATOR, TabletTypes.FLAT_HIVE,
            TabletTypes.FLAT_BS_CONTROLLER, TabletTypes.FLAT_SCHEMESHARD, TabletTypes.TX_ALLOCATOR
        ]
        for tablet_type in tablet_types:
            response = self.client.tablet_state(tablet_type)
            tablets_info = response.TabletStateInfo

            assert_that(
                tablets_info,
                has_length(greater_than(0)),
                "There are no tablets with given tablet_type"
            )

            # Act
            tablet_id = tablets_info[0].TabletId
            prev_generation = tablets_info[0].Generation
            self.client.tablet_kill(tablet_id)
            wait_tablets_state_by_id(
                self.client,
                TabletStates.Active,
                tablet_ids=[tablet_id],
                skip_generations={tablet_id: prev_generation},
                generation_matcher=greater_than,
                message='Killed tablet is active again and increased generation',
                timeout_seconds=TIMEOUT_SECONDS,
            )
