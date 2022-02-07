#!/usr/bin/env python
# -*- coding: utf-8 -*-

from enum import IntEnum, unique

from ydb.core.protos.flat_scheme_op_pb2 import EPathType


@unique
class PathType(IntEnum):
    """
    These are from /arcadia/ydb/core/protos/flat_scheme_op.proto
    Just remap
    """

    Dir = EPathType.Value('EPathTypeDir')
    Table = EPathType.Value('EPathTypeTable')
    PersQueue = EPathType.Value('EPathTypePersQueueGroup')
    SubDomain = EPathType.Value('EPathTypeSubDomain')
