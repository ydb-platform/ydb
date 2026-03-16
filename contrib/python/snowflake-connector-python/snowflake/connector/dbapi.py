#!/usr/bin/env python
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

"""This module implements some constructors and singletons as required by the DB API v2.0 (PEP-249)."""

from __future__ import annotations

import datetime
import json
import time

from .constants import (
    get_binary_types,
    get_number_types,
    get_string_types,
    get_timestamp_types,
)
from .mixin import UnicodeMixin


class _DBAPITypeObject:
    def __init__(self, *values):
        self.values = values

    def __cmp__(self, other):
        if other in self.values:
            return 0
        if other < self.values:
            return 1
        else:
            return -1


Date = datetime.date
Time = datetime.time
Timestamp = datetime.datetime


def DateFromTicks(ticks):
    return Date(*time.localtime(ticks)[:3])


def TimeFromTicks(ticks):
    return Time(*time.localtime(ticks)[3:6])


def TimestampFromTicks(ticks):
    return Timestamp(*time.localtime(ticks)[:6])


Binary = bytes

STRING = _DBAPITypeObject(get_string_types())
BINARY = _DBAPITypeObject(get_binary_types())
NUMBER = _DBAPITypeObject(get_number_types())
DATETIME = _DBAPITypeObject(get_timestamp_types())
ROWID = _DBAPITypeObject()


class Json(UnicodeMixin):
    """JSON adapter."""

    def __init__(self, value):
        self._value = json.dump(value)

    def __repr__(self):
        return self.__str__()

    def __unicode__(self):
        return self._value
