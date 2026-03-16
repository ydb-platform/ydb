#!/usr/bin/env python
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

from logging import getLogger

import pytest

from snowflake.connector import ProgrammingError
from snowflake.connector.connection import DefaultConverterClass
from snowflake.connector.converter import SnowflakeConverter
from snowflake.connector.converter_snowsql import SnowflakeConverterSnowSQL

logger = getLogger(__name__)

ConverterSnowSQL = SnowflakeConverterSnowSQL


def test_is_dst():
    """SNOW-6020: Failed to convert to local time during DST is being changed."""
    # DST to non-DST
    convClass = DefaultConverterClass()
    conv = convClass()
    conv.set_parameter("TIMEZONE", "America/Los_Angeles")

    col_meta = {
        "name": "CREATED_ON",
        "type": 6,
        "length": None,
        "precision": None,
        "scale": 3,
        "nullable": True,
    }
    m = conv.to_python_method("TIMESTAMP_LTZ", col_meta)
    ret = m("1414890189.000")

    assert (
        str(ret) == "2014-11-01 18:03:09-07:00"
    ), "Timestamp during from DST to non-DST"

    # non-DST to DST
    col_meta = {
        "name": "CREATED_ON",
        "type": 6,
        "length": None,
        "precision": None,
        "scale": 3,
        "nullable": True,
    }
    m = conv.to_python_method("TIMESTAMP_LTZ", col_meta)
    ret = m("1425780189.000")

    assert (
        str(ret) == "2015-03-07 18:03:09-08:00"
    ), "Timestamp during from non-DST to DST"


def test_more_timestamps():
    conv = ConverterSnowSQL()
    conv.set_parameter("TIMESTAMP_NTZ_OUTPUT_FORMAT", "YYYY-MM-DD HH24:MI:SS.FF9")
    m = conv.to_python_method("TIMESTAMP_NTZ", {"scale": 9})
    assert m("-2208943503.876543211") == "1900-01-01 12:34:56.123456789"
    assert m("-2208943503.000000000") == "1900-01-01 12:34:57.000000000"
    assert m("-2208943503.012000000") == "1900-01-01 12:34:56.988000000"

    conv.set_parameter("TIMESTAMP_NTZ_OUTPUT_FORMAT", "YYYY-MM-DD HH24:MI:SS.FF9")
    m = conv.to_python_method("TIMESTAMP_NTZ", {"scale": 7})
    assert m("-2208943503.8765432") == "1900-01-01 12:34:56.123456800"
    assert m("-2208943503.0000000") == "1900-01-01 12:34:57.000000000"
    assert m("-2208943503.0120000") == "1900-01-01 12:34:56.988000000"


def test_converter_to_snowflake_error():
    converter = SnowflakeConverter()
    with pytest.raises(
        ProgrammingError, match=r"Binding data in type \(bogus\) is not supported"
    ):
        converter._bogus_to_snowflake("Bogus")


def test_converter_to_snowflake_bindings_error():
    converter = SnowflakeConverter()
    with pytest.raises(
        ProgrammingError,
        match=r"Binding data in type \(somethingsomething\) is not supported",
    ):
        converter._somethingsomething_to_snowflake_bindings("Bogus")
