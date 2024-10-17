#!/usr/bin/env python
# -*- coding: utf-8 -*-
import six

from hamcrest import all_of, has_property, has_properties
from hamcrest.core.base_matcher import BaseMatcher

import ydb.core.protos.msgbus_kv_pb2 as msgbus_kv
from ydb.tests.library.common.msgbus_types import EReplyStatus, TStorageStatusFlags
from ydb.tests.library.common.msgbus_types import MessageBusStatus
from ydb.tests.library.matchers.collection import contains
from ydb.tests.library.matchers.response_matchers import ProtobufWithStatusMatcher


def to_bytes(v):
    if v is None:
        return None

    if not isinstance(v, six.binary_type):
        try:
            return bytes(v, 'utf-8')
        except Exception as e:
            raise ValueError(str(e), type(v))
    return v


class KeyValueResponseProtobufMatcher(BaseMatcher):

    def __init__(self, status=MessageBusStatus.MSTATUS_OK, do_describe=True):
        self.__read_range_matchers = []
        self.__read_keys_matchers = []
        self.__write_matchers = []
        self.__rename_matchers = []
        self.__delete_matcher = None
        self.__copy_range_matchers = []
        self.__concat_keys_result_matchers = []
        self.__inc_gen_result_matcher = None
        self.__storage_channel_result_matchers = []
        self.__do_describe = do_describe
        self.__describe_actual_proto_fields = set()
        self.__status = status

    def no_describe(self, describe_fields=()):
        self.__do_describe = False
        self.__describe_actual_proto_fields = set(describe_fields)
        return self

    def describe_to(self, description):
        description.append_text('Valid response with Status: %i' % self.__status)
        if self.__do_describe:
            if self.__inc_gen_result_matcher is not None:
                description.append_text(' and IncrementGenerationResult = \n')
                self.__inc_gen_result_matcher.describe_to(description)
            if self.__write_matchers:
                description.append_text(' and WriteResult = \n')
                contains(*self.__write_matchers).describe_to(description)
            if self.__delete_matcher is not None:
                description.append_text(' and DeleteRangeResult = \n')
                self.__delete_matcher.describe_to(description)
            if self.__read_keys_matchers:
                description.append_text(' and ReadResult = \n')
                contains(*self.__read_keys_matchers).describe_to(description)
            if self.__read_range_matchers:
                description.append_text(' and ReadRangeResult = \n')
                contains(*self.__read_range_matchers).describe_to(description)
            if self.__copy_range_matchers:
                description.append_text(' and CopyRangeResult = \n')
                contains(*self.__copy_range_matchers).describe_to(description)
            if self.__concat_keys_result_matchers:
                description.append_text(' and ConcatResult = \n')
                contains(*self.__concat_keys_result_matchers).describe_to(description)
        else:
            description.append_text("(Verbose mismatch description was disabled for this test)")

    def describe_mismatch(self, actual_protobuf, mismatch_description):
        if self.__do_describe and not self.__describe_actual_proto_fields:
            actual_protobuf_str = str(actual_protobuf)
        else:
            lst = []
            field_names = [f.name for f in actual_protobuf.DESCRIPTOR.fields]
            for f in field_names:
                if f in self.__describe_actual_proto_fields:
                    value = getattr(actual_protobuf, f)
                    if not isinstance(value, six.text_type):
                        try:
                            value = list([v for v in value])
                        except TypeError:
                            pass
                    lst.append("{f} = {v}".format(f=f, v=value))
            lst.append('\n')
            actual_protobuf_str = '\n'.join(lst)

        mismatch_description.append_text("Actual protobuf = \n")
        mismatch_description.append_text("=" * 60 + "\n")
        mismatch_description.append_text(actual_protobuf_str)
        mismatch_description.append_text("=" * 60 + "\n")

    def _matches(self, actual_protobuf):
        if not hasattr(actual_protobuf, 'Status') or actual_protobuf.Status != self.__status:
            return False

        if self.__inc_gen_result_matcher is not None\
                and not self.__inc_gen_result_matcher.matches(actual_protobuf.IncrementGenerationResult):
            return False

        if (
            (self.__read_range_matchers and
             not contains(*self.__read_range_matchers).matches(actual_protobuf.ReadRangeResult)
             ) or
            (self.__rename_matchers and
             not contains(*self.__rename_matchers).matches(actual_protobuf.RenameResult)
             ) or
            (self.__write_matchers and
                not contains(*self.__write_matchers).matches(actual_protobuf.WriteResult)
             ) or
            (self.__delete_matcher is not None and
                not self.__delete_matcher.matches(actual_protobuf.DeleteRangeResult)
             ) or
            (self.__copy_range_matchers and
                not contains(*self.__copy_range_matchers).matches(actual_protobuf.CopyRangeResult)
             ) or
            (self.__concat_keys_result_matchers and
                not contains(*self.__concat_keys_result_matchers).matches(actual_protobuf.ConcatResult)
             ) or
            (self.__read_keys_matchers and
                not contains(*self.__read_keys_matchers).matches(actual_protobuf.ReadResult)
             ) or
            (self.__storage_channel_result_matchers and
                not contains(*self.__storage_channel_result_matchers).matches(actual_protobuf.GetStatusResult)
             )
        ):
            return False

        return True

    def read_range(self, key_value_pairs, status=EReplyStatus.OK):
        key_value_pair_matchers = tuple(kv_pair.protobuf_matcher() for kv_pair in key_value_pairs)
        self.__read_range_matchers.append(
            has_properties(
                Status=status,
                Pair=contains(*tuple(
                    key_value_pair_matchers
                ))
            )
        )
        return self

    def read_key(self, value, status=EReplyStatus.OK):
        self.__read_keys_matchers.append(
            has_properties(
                Status=status,
                Value=to_bytes(value)
            )
        )
        return self

    def write(self, num_of_writes=1):
        self.__write_matchers.extend(
            has_properties(Status=int(EReplyStatus.OK), StatusFlags=int(TStorageStatusFlags.StatusIsValid))
            for _ in range(num_of_writes)
        )
        return self

    def add_write_result_with_status(self, status, flags):
        self.__write_matchers.append(
            has_properties(Status=int(status), StatusFlags=int(flags))
        )
        return self

    def rename(self, num_of_renames=1):
        self.__rename_matchers.extend(
            has_property('Status', int(EReplyStatus.OK)) for _ in range(num_of_renames)
        )
        return self

    def delete(self, num_of_keys=1):
        self.__delete_matcher = contains(*tuple(
            has_property('Status', EReplyStatus.OK) for _ in range(num_of_keys)
        ))
        return self

    def copy_range(self, num_of_ranges_copied=1):
        self.__copy_range_matchers.extend(
            has_property('Status', EReplyStatus.OK) for _ in range(num_of_ranges_copied)
        )
        return self

    def concat_keys(self):
        self.__concat_keys_result_matchers.append(
            has_property('Status', EReplyStatus.OK)
        )
        return self

    def inc_generation(self, new_gen):
        self.__inc_gen_result_matcher = has_properties(
            Status=EReplyStatus.OK,
            Generation=new_gen
        )
        return self

    def add_storage_channel_status_result(
            self,
            flags=TStorageStatusFlags.StatusIsValid,
            storage_channel_type=msgbus_kv.TKeyValueRequest.MAIN,
            status=EReplyStatus.OK
    ):
        self.__storage_channel_result_matchers.append(
            has_properties(
                Status=status,
                StorageChannel=storage_channel_type,
                StatusFlags=int(flags)
            )
        )
        return self


def is_response_with_status(status):
    return ProtobufWithStatusMatcher(status)


def is_ok_response():
    return is_response_with_status(MessageBusStatus.MSTATUS_OK)


def is_valid_response_with_field(field_name, field_matcher):
    return all_of(
        has_property('Status', MessageBusStatus.MSTATUS_OK),
        has_property(field_name, field_matcher)
    )


def is_response_with_status_and_fields(status, **kwargs):
    return all_of(
        is_response_with_status(status),
        has_properties(**kwargs)
    )


def is_valid_read_range_response(key_value_pairs,
                                 main_status=MessageBusStatus.MSTATUS_OK, result_status=EReplyStatus.OK):
    return KeyValueResponseProtobufMatcher(main_status).read_range(key_value_pairs, status=result_status)


def is_valid_keyvalue_protobuf_response():
    return KeyValueResponseProtobufMatcher()
