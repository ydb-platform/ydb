#!/usr/bin/env python
# -*- coding: utf-8 -*-

from ydb.tests.library.common.msgbus_types import MessageBusStatus
from ydb.tests.library.matchers.response_matchers import AbstractProtobufMatcher, FakeProtobuf


class TResponseMatcher(AbstractProtobufMatcher):

    def __init__(self, status=MessageBusStatus.MSTATUS_OK):
        super(TResponseMatcher, self).__init__()
        self.expected_protobuf.Status = status

    def with_minikql_compile_results(self, message):
        if 'ProgramCompileErrors' not in self.expected_protobuf.MiniKQLCompileResults:
            self.expected_protobuf.MiniKQLCompileResults.ProgramCompileErrors = []

        self.expected_protobuf.MiniKQLCompileResults.ProgramCompileErrors.append(
            FakeProtobuf(message=message)
        )
        return self
