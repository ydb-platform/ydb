#!/usr/bin/env python
# -*- coding: utf-8 -*-
import enum


@enum.unique
class MessageBusStatus(enum.IntEnum):
    """
    Message Bus response statuses.

    See /arcadia/kikimr/core/client/base/msgbus.h

    See /arcadia/ydb/core/protos/msgbus.proto
    """

    # These are from /arcadia/kikimr/core/client/base/msgbus.h
    MSTATUS_UNKNOWN = 0
    MSTATUS_OK = 1
    MSTATUS_ERROR = 128
    MSTATUS_INPROGRESS = 129
    MSTATUS_TIMEOUT = 130
    MSTATUS_NOTREADY = 131
    MSTATUS_ABORTED = 132
    MSTATUS_INTERNALERROR = 133
    MSTATUS_REJECTED = 134

    @staticmethod
    def is_ok_status(status):
        return status in (
            MessageBusStatus.MSTATUS_OK,
            MessageBusStatus.MSTATUS_INPROGRESS
        )


@enum.unique
class EMessageStatus(enum.IntEnum):
    """
    See /arcadia/library/cpp/messagebus/message_status.h
    """
    MESSAGE_OK = 0
    MESSAGE_CONNECT_FAILED = 1
    MESSAGE_TIMEOUT = 2
    MESSAGE_SERVICE_UNKNOWN = 3
    MESSAGE_BUSY = 4
    MESSAGE_UNKNOWN = 5
    MESSAGE_DESERIALIZE_ERROR = 6
    MESSAGE_HEADER_CORRUPTED = 7
    MESSAGE_DECOMPRESS_ERROR = 8
    MESSAGE_MESSAGE_TOO_LARGE = 9
    MESSAGE_REPLY_FAILED = 10
    MESSAGE_DELIVERY_FAILED = 11
    MESSAGE_INVALID_VERSION = 12
    MESSAGE_SERVICE_TOOMANY = 13
    MESSAGE_SHUTDOWN = 14
    MESSAGE_DONT_ASK = 15


@enum.unique
class EReplyStatus(enum.IntEnum):
    """
    See /arcadia/ydb/core/protos/base.proto
    """
    OK = 0
    ERROR = 1
    ALREADY = 2
    TIMEOUT = 3
    RACE = 4
    NODATA = 5
    BLOCKED = 6
    NOTREADY = 7
    OVERRUN = 8
    TRYLATER = 9
    TRYLATER_TIME = 10
    TRYLATER_SIZE = 11
    DEADLINE = 12
    CORRUPTED = 13
    UNKNOWN = 255


@enum.unique
class TStorageStatusFlags(enum.IntEnum):
    """
    See /arcadia/kikimr/core/base/blobstorage.h
    TStorageStatusFlags::EStatus
    """
    StatusIsValid = 1,
    StatusDiskSpaceYellow = 1 << 1,
    StatusDiskSpaceOrange = 1 << 2,
    StatusDiskSpaceRed = 1 << 3


@enum.unique
class SchemeStatus(enum.IntEnum):
    """
    See /arcadia/ydb/core/protos/flat_tx_scheme.proto
    enum EStatus
    """
    StatusSuccess = 0
    StatusAccepted = 1
    StatusPathDoesNotExist = 2
    StatusPathIsNotDirectory = 3
    StatusAlreadyExists = 4
    StatusSchemeError = 5
    StatusNameConflict = 6
    StatusInvalidParameter = 7
    StatusMultipleModifications = 8
    ProxyShardNotAvailable = 13


@enum.unique
class EDriveStatus(enum.IntEnum):
    UNKNOWN = 0
    ACTIVE = 1
    INACTIVE = 2
    BROKEN = 3
    SPARE = 4
    FAULTY = 5
