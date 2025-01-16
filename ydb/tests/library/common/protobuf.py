#!/usr/bin/env python
# -*- coding: utf-8 -*-

import abc
from collections import namedtuple
import six
from hamcrest import has_properties

import ydb.core.protos.msgbus_pb2 as msgbus


def build_protobuf_if_necessary(protobuf_builder):
    if hasattr(protobuf_builder, 'protobuf'):
        return protobuf_builder.protobuf
    else:
        return protobuf_builder


class AbstractProtobufBuilder(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, protobuf_object):
        super(AbstractProtobufBuilder, self).__init__()
        self.__protobuf = protobuf_object
        self.__hash = None

    @property
    def protobuf(self):
        return self.__protobuf

    def __hash__(self):
        if self.__hash is None:
            self.__hash = hash(str(self.protobuf))
        return self.__hash


def to_bytes(v):
    if v is None:
        return None

    if not isinstance(v, six.binary_type):
        try:
            return bytes(v, 'utf-8')
        except Exception as e:
            raise ValueError(str(e), type(v))
    return v


class THiveCreateTablet(AbstractProtobufBuilder):

    def __init__(self, domain_id=1):
        super(THiveCreateTablet, self).__init__(msgbus.THiveCreateTablet())
        self.protobuf.DomainUid = domain_id

    def create_tablet(self, owner_id, owner_idx, tablet_type, channels_profile=0, allowed_node_ids=()):
        create_tablet_cmd = self.protobuf.CmdCreateTablet.add()
        create_tablet_cmd.OwnerId = owner_id
        create_tablet_cmd.OwnerIdx = owner_idx
        create_tablet_cmd.TabletType = int(tablet_type)
        create_tablet_cmd.ChannelsProfile = channels_profile
        if allowed_node_ids:
            create_tablet_cmd.AllowedNodeIDs.extend(allowed_node_ids)
        return self

    def lookup_tablet(self, owner_id, owner_idx):
        self.protobuf.CmdLookupTablet.add(
            OwnerId=owner_id,
            OwnerIdx=owner_idx
        )
        return self


class TCmdCreateTablet(
    namedtuple('TCmdCreateTablet', ['owner_id', 'owner_idx', 'type', 'allowed_node_ids', 'channels_profile', 'binded_channels'])
):
    def __new__(cls, owner_id, owner_idx, type, allowed_node_ids=(), channels_profile=0, binded_channels=None):
        return super(TCmdCreateTablet, cls).__new__(cls, owner_id, owner_idx, type, allowed_node_ids, channels_profile, binded_channels)


TCmdWrite = namedtuple('TCmdWrite', ['key', 'value'])


class TCmdRead(namedtuple('TCmdRead', ['key', 'offset', 'size'])):
    pass

    @staticmethod
    def full_key(key):
        return TCmdRead(key=key, offset=0, size=0)


class TKeyRange(
        namedtuple('TKeyRange', ['from_key', 'to_key', 'include_from', 'include_to', 'include_data', 'limit_bytes'])):
    def __new__(cls, from_key, to_key, include_from=True, include_to=True, include_data=True, limit_bytes=None):
        return super(TKeyRange, cls).__new__(cls, to_bytes(from_key), to_bytes(to_key), include_from, include_to, include_data, limit_bytes)

    @staticmethod
    def range(func, from_, to_, include_from=True, include_to=True):
        from_ = func(from_)
        to_ = func(to_)
        return TKeyRange(from_, to_, include_from, include_to)

    @staticmethod
    def full_range():
        from_ = ''
        to_ = chr(255)
        return TKeyRange(from_, to_)

    def __contains__(self, item):
        return (
            (
                self.include_from and self.from_key <= item
                or not self.include_from and self.from_key < item
            ) and (
                self.include_to and item <= self.to_key
                or not self.include_to and item < self.to_key
            )
        )


class TKeyValuePair(namedtuple('TKeyValuePair', ['key', 'value', 'size', 'creation_time'])):
    """
    See TKeyValuePair from ydb/core/protos/msgbus_kv.proto
    """
    def __new__(cls, key, value, size=None, creation_time=None):
        if size is None and value is not None:
            size = len(value)
        return super(TKeyValuePair, cls).__new__(cls, to_bytes(key), to_bytes(value), size, creation_time)

    def __cmp__(self, other):
        return self.key - other.key

    def protobuf_matcher(self):
        if self.creation_time is not None and self.size is not None and self.value is not None:
            return has_properties(
                Key=self.key,
                Value=self.value,
                ValueSize=self.size,
                CreationUnixTime=self.creation_time
            )
        elif self.value is not None:
            return has_properties(
                Key=self.key,
                Value=self.value,
                ValueSize=self.size
            )
        elif self.size is not None:
            return has_properties(
                Key=self.key,
                ValueSize=self.size
            )
        else:
            return has_properties(
                Key=self.key
            )


TCmdRename = namedtuple('TCmdRename', ['old_key', 'new_key'])


class TSchemeDescribe(AbstractProtobufBuilder):
    def __init__(self, path):
        super(TSchemeDescribe, self).__init__(msgbus.TSchemeDescribe())
        self.protobuf.Path = path
