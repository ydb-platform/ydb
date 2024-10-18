# -*- coding: utf-8 -*-
from hamcrest import assert_that, has_properties

from ydb.tests.library.common.delayed import wait_tablets_are_active
from ydb.tests.library.common.msgbus_types import EReplyStatus
from ydb.tests.library.common.protobuf import TCmdCreateTablet, THiveCreateTablet
from ydb.tests.library.common.types import Erasure, TabletTypes
from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.matchers.collection import contains
from ydb.tests.library.matchers.response_matchers import DynamicFieldsProtobufMatcher

from hive_matchers import CreateTabletsResponseMatcher, TLocalEnumerateTabletsResultMatcher
from ydb.tests.library.kv.helpers import create_tablets_and_wait_for_start

TIMEOUT_SECONDS = 240


class TestHive(object):
    @classmethod
    def setup_class(cls):
        configurator = KikimrConfigGenerator(Erasure.BLOCK_4_2, nodes=8)
        cls.cluster = kikimr_cluster_factory(configurator=configurator)
        cls.cluster.start()
        cls.client = cls.cluster.client

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, 'cluster'):
            cls.cluster.stop()

    def test_when_kill_node_with_tablets_when_all_tablets_start_only_on_allowed_nodes(self):
        # Arrange
        tablet_ids_first = create_tablets_and_wait_for_start(
            self.client, owner_id=2474, number_of_tablets=10, allowed_node_ids=[2, 3])
        tablet_ids_second = create_tablets_and_wait_for_start(
            self.client, owner_id=4645, number_of_tablets=10, allowed_node_ids=[2, 4])

        # Act
        self.cluster.nodes[2].stop()
        wait_tablets_are_active(self.client, tablet_ids_first + tablet_ids_second)

        # Assert
        response_first = self.client.local_enumerate_tablets(node_id=3, tablet_type=TabletTypes.KEYVALUEFLAT)
        assert_that(
            response_first,
            TLocalEnumerateTabletsResultMatcher().tablet_info(
                [
                    has_properties(TabletId=tablet_id)
                    for tablet_id in tablet_ids_first
                ]
            )
        )

        response_second = self.client.local_enumerate_tablets(node_id=4, tablet_type=TabletTypes.KEYVALUEFLAT)
        assert_that(
            response_second,
            TLocalEnumerateTabletsResultMatcher().tablet_info(
                [
                    has_properties(TabletId=tablet_id)
                    for tablet_id in tablet_ids_second
                ]
            )
        )

    def test_when_create_tablets_after_bs_groups_and_kill_hive_then_tablets_start(self):
        owner_id = 453
        num_of_tablets_in_batch = 5

        # Arrange
        response = self.client.hive_create_tablets(
            [
                TCmdCreateTablet(owner_id=owner_id, owner_idx=index, type=TabletTypes.KEYVALUEFLAT,
                                 channels_profile=1)
                for index in range(num_of_tablets_in_batch)
                ]
        )
        assert_that(
            response,
            CreateTabletsResponseMatcher().create_tablet_result(num_of_tablets_in_batch)
        )
        tablet_ids = [tablet.TabletId for tablet in response.CreateTabletResult]

        self.cluster.add_storage_pool(erasure=Erasure.MIRROR_3)
        self.client.tablet_kill(TabletTypes.FLAT_HIVE.tablet_id_for(0))

        wait_tablets_are_active(self.client, tablet_ids)

    def test_when_create_tablets_then_can_lookup_them(self):
        num_of_tablets_to_create = 2
        owner_id = 1
        tablet_ids = create_tablets_and_wait_for_start(
            self.client,
            owner_id=owner_id, number_of_tablets=num_of_tablets_to_create)

        lookup_request = THiveCreateTablet()
        for index in range(num_of_tablets_to_create + 1):
            lookup_request.lookup_tablet(owner_id=owner_id, owner_idx=index)

        response = self.client.send_request(lookup_request.protobuf, method='HiveCreateTablet')

        # Assert
        expected_list = [
            has_properties(Status=EReplyStatus.OK, TabletId=tablet_id)
            for tablet_id in tablet_ids
        ] + [has_properties(Status=EReplyStatus.NODATA)]

        assert_that(
            response,
            DynamicFieldsProtobufMatcher().LookupTabletResult(
                contains(*expected_list)
            )
        )
