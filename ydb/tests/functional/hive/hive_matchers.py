#!/usr/bin/env python
# -*- coding: utf-8 -*-
from hamcrest import anything, contains_inanyorder

from ydb.tests.library.common.msgbus_types import EReplyStatus
from ydb.tests.library.common.msgbus_types import MessageBusStatus
from ydb.tests.library.matchers.response_matchers import AbstractProtobufMatcher, FakeProtobuf


class CreateTabletsResponseMatcher(AbstractProtobufMatcher):

    def __init__(self):
        super(CreateTabletsResponseMatcher, self).__init__()
        self.expected_protobuf.Status = MessageBusStatus.MSTATUS_OK
        self.expected_protobuf.CreateTabletResult = []

    def create_tablet_result(self, number_of_tablets_created):
        self.expected_protobuf.CreateTabletResult.extend(
            [FakeProtobuf(Status=EReplyStatus.OK, TabletId=anything()) for _ in range(number_of_tablets_created)]
        )
        return self


class TEvTabletStateResponseMatcher(AbstractProtobufMatcher):
    def __init__(self):
        super(TEvTabletStateResponseMatcher, self).__init__()
        self.expected_protobuf.Status = MessageBusStatus.MSTATUS_OK

    def tablet_state_info(self, set_of_tablet_states):
        self.expected_protobuf.TabletStateInfo = contains_inanyorder(*set_of_tablet_states)
        return self


class TLocalEnumerateTabletsResultMatcher(AbstractProtobufMatcher):
    def __init__(self):
        super(TLocalEnumerateTabletsResultMatcher, self).__init__()
        self.expected_protobuf.Status = MessageBusStatus.MSTATUS_OK

    def tablet_info(self, set_of_tablet_info):
        self.expected_protobuf.TabletInfo = contains_inanyorder(*set_of_tablet_info)
        return self
