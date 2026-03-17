#!/usr/bin/env python
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

import time
from datetime import datetime

import pytest

from snowflake.connector.compat import IS_WINDOWS
from snowflake.connector.sfdatetime import SnowflakeDateTime, SnowflakeDateTimeFormat


def test_basic_datetime_format():
    """Datetime format basic tests."""
    # date
    value = datetime(2014, 11, 30)
    formatter = SnowflakeDateTimeFormat("YYYY-MM-DD")
    assert formatter.format(value) == "2014-11-30"

    # date time => date
    value = datetime(2014, 11, 30, 12, 31, 45)
    formatter = SnowflakeDateTimeFormat("YYYY-MM-DD")
    assert formatter.format(value) == "2014-11-30"

    # date time => date time
    value = datetime(2014, 11, 30, 12, 31, 45)
    formatter = SnowflakeDateTimeFormat('YYYY-MM-DD"T"HH24:MI:SS')
    assert formatter.format(value) == "2014-11-30T12:31:45"

    # date time => date time in microseconds with 4 precision
    value = datetime(2014, 11, 30, 12, 31, 45, microsecond=987654)
    formatter = SnowflakeDateTimeFormat('YYYY-MM-DD"T"HH24:MI:SS.FF4')
    assert formatter.format(value) == "2014-11-30T12:31:45.9876"

    # date time => date time in microseconds with full precision up to
    # microseconds
    value = datetime(2014, 11, 30, 12, 31, 45, microsecond=987654)
    formatter = SnowflakeDateTimeFormat('YYYY-MM-DD"T"HH24:MI:SS.FF')
    assert formatter.format(value) == "2014-11-30T12:31:45.987654"


def test_datetime_with_smaller_milliseconds():
    # date time => date time in microseconds with full precision up to
    # microseconds
    value = datetime(2014, 11, 30, 12, 31, 45, microsecond=123)
    formatter = SnowflakeDateTimeFormat('YYYY-MM-DD"T"HH24:MI:SS.FF9')
    assert formatter.format(value) == "2014-11-30T12:31:45.000123"


def test_datetime_format_negative():
    """Datetime format negative."""
    value = datetime(2014, 11, 30, 12, 31, 45, microsecond=987654)
    formatter = SnowflakeDateTimeFormat('YYYYYYMMMDDDDD"haha"hoho"hihi"H12HHH24MI')
    assert formatter.format(value) == "20141411M3030DhahaHOHOhihiH1212H2431"


def test_struct_time_format():
    # struct_time for general use
    value = time.strptime("30 Sep 01 11:20:30", "%d %b %y %H:%M:%S")
    formatter = SnowflakeDateTimeFormat('YYYY-MM-DD"T"HH24:MI:SS.FF')
    assert formatter.format(value) == "2001-09-30T11:20:30.0"

    # struct_time encapsulated in SnowflakeDateTime. Mainly used by SnowSQL
    value = SnowflakeDateTime(
        time.strptime("30 Sep 01 11:20:30", "%d %b %y %H:%M:%S"), nanosecond=0, scale=1
    )
    formatter = SnowflakeDateTimeFormat(
        'YYYY-MM-DD"T"HH24:MI:SS.FF', datetime_class=SnowflakeDateTime
    )
    assert formatter.format(value) == "2001-09-30T11:20:30.0"

    # format without fraction of seconds
    formatter = SnowflakeDateTimeFormat(
        'YYYY-MM-DD"T"HH24:MI:SS', datetime_class=SnowflakeDateTime
    )
    assert formatter.format(value) == "2001-09-30T11:20:30"


@pytest.mark.skipif(IS_WINDOWS, reason="not supported yet")
def test_struct_time_format_extreme_large():
    # extreme large epoch time
    value = SnowflakeDateTime(time.gmtime(14567890123567), nanosecond=0, scale=1)
    formatter = SnowflakeDateTimeFormat(
        'YYYY-MM-DD"T"HH24:MI:SS.FF', datetime_class=SnowflakeDateTime
    )
    assert formatter.format(value) == "463608-01-23T09:26:07.0"
