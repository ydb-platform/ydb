#!/usr/bin/env python
# -*- coding: utf-8 -*-
from hamcrest import assert_that

from ydb.tests.library.common.delayed import wait_tablets_state_by_id
from ydb.tests.library.common.protobuf import TCmdCreateTablet
from ydb.tests.library.common.types import Erasure, TabletTypes, TabletStates
from ydb.tests.library.harness.util import LogLevels
from ydb.tests.tools.nemesis.library.tablet import BulkChangeTabletGroupNemesis
from ydb.tests.tools.nemesis.library.tablet import KillHiveNemesis, KillBsControllerNemesis
from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.matchers.tablets import all_tablets_are_created

TIMEOUT_SECONDS = 480
TABLETS_PER_NODE = 100


class TestMassiveKills(object):
    @classmethod
    def setup_class(cls):
        cls.configurator = KikimrConfigGenerator(
            erasure=Erasure.BLOCK_4_2,
            additional_log_configs={
                'HIVE': LogLevels.DEBUG,
                'LOCAL': LogLevels.DEBUG,
                'BS_CONTROLLER': LogLevels.DEBUG,
            }
        )
        cls.cluster = kikimr_cluster_factory(configurator=cls.configurator)
        cls.cluster.start()

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, 'cluster'):
            cls.cluster.stop()

    def test_tablets_are_ok_after_many_kills(self):
        nodes_count = len(self.cluster.nodes.values())
        cmd_create_tablets = [
            TCmdCreateTablet(owner_id=234843, owner_idx=index, type=TabletTypes.KEYVALUEFLAT)
            for index in range(nodes_count * TABLETS_PER_NODE)
        ]

        create_response = self.cluster.client.hive_create_tablets(cmd_create_tablets)
        all_tablet_ids = set([tablet.TabletId for tablet in create_response.CreateTabletResult])

        assert_that(create_response, all_tablets_are_created(cmd_create_tablets))

        wait_tablets_state_by_id(
            self.cluster.client, TabletStates.Active, tablet_ids=all_tablet_ids, timeout_seconds=TIMEOUT_SECONDS
        )
        nemesis = [
            BulkChangeTabletGroupNemesis(self.cluster, TabletTypes.KEYVALUEFLAT),
            KillHiveNemesis(self.cluster),
            KillBsControllerNemesis(self.cluster),
        ]

        for element in nemesis:
            element.prepare_state()

        for _ in range(3):
            for element in nemesis:
                element.inject_fault()

        for tablet_id in all_tablet_ids:
            self.cluster.client.tablet_kill(tablet_id)

        wait_tablets_state_by_id(
            self.cluster.client,
            TabletStates.Active,
            tablet_ids=all_tablet_ids,
            timeout_seconds=TIMEOUT_SECONDS,
        )
