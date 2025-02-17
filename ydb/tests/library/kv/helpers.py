# -*- coding: utf-8 -*-
from hamcrest import assert_that, contains, has_properties, any_of
import six.moves
import logging
import time

from ydb.tests.library.common.delayed import wait_tablets_state_by_id
from ydb.tests.library.common.msgbus_types import MessageBusStatus, EReplyStatus
from ydb.tests.library.common.protobuf import TCmdCreateTablet
from ydb.tests.library.common.types import TabletTypes, TabletStates
from ydb.tests.library.matchers.response_matchers import DynamicFieldsProtobufMatcher
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds

logger = logging.getLogger()


def create_tablets_and_wait_for_start(
        kikimr_client, number_of_tablets, batch_size=1, owner_id=2421, timeout_seconds=120, skip_generations=None,
        tablet_type=None, binded_channels=None, allowed_node_ids=None
):
    all_tablet_ids = []
    for batch_id, owner_idx_start in enumerate(six.moves.xrange(0, number_of_tablets, batch_size)):
        owner_indexes = range(owner_idx_start, owner_idx_start + batch_size)
        tablet_type = tablet_type if tablet_type is not None else TabletTypes.KEYVALUEFLAT
        tablets = [
            TCmdCreateTablet(
                owner_id=owner_id, owner_idx=owner_idx,
                type=tablet_type, binded_channels=binded_channels, allowed_node_ids=allowed_node_ids)
            for owner_idx in owner_indexes
        ]
        response = kikimr_client.hive_create_tablets(tablets)

        assert_that(
            response,
            DynamicFieldsProtobufMatcher()
            .Status(MessageBusStatus.MSTATUS_OK)
            .CreateTabletResult(
                contains(
                    *[
                        has_properties(Status=any_of(EReplyStatus.OK, EReplyStatus.ALREADY))
                        for _ in owner_indexes
                    ]
                )
            )
        )
        tablet_ids = [x.TabletId for x in response.CreateTabletResult]
        all_tablet_ids.extend(tablet_ids)

        wait_tablets_state_by_id(
            kikimr_client,
            TabletStates.Active,
            tablet_ids=tablet_ids,
            timeout_seconds=timeout_seconds,
            message='batch_id = ' + str(batch_id),
            skip_generations=skip_generations,
        )

    return all_tablet_ids


def create_kv_tablets_and_wait_for_start(
        kikimr_client, kv_client, swagger_client, number_of_tablets, path, timeout_seconds=120, skip_generations=None
):
    response = kv_client.create_tablets(number_of_tablets, path)
    assert_that(response.operation.status == StatusIds.SUCCESS)
    all_tablet_ids = get_kv_tablet_ids(swagger_client)
    wait_tablets_state_by_id(
        kikimr_client,
        TabletStates.Active,
        tablet_ids=all_tablet_ids,
        timeout_seconds=timeout_seconds,
        skip_generations=skip_generations,
    )
    return all_tablet_ids


def get_kv_tablet_ids(swagger_client, retry_count=4, retry_min_sleep=0.1, retry_max_sleep=5):
    data = swagger_client.tablet_info('KeyValue')
    tablet_state_info = {}
    retries = retry_count
    next_sleep = retry_min_sleep
    while True:
        try:
            data = swagger_client.tablet_info('KeyValue')
            tablet_state_info = data['TabletStateInfo']
            break
        except (KeyError):
            retries -= 1
            if retries <= 0:
                raise
            time.sleep(next_sleep)
            next_sleep = min(next_sleep * 2, retry_max_sleep)
    tablet_ids = list()
    for tablet_info in tablet_state_info:
        tablet_ids.append(int(tablet_info['TabletId']))
    tablet_ids.sort()
    return tablet_ids
