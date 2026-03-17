#!/usr/bin/env python
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

from snowflake.connector.sfbinaryformat import (
    SnowflakeBinaryFormat,
    binary_to_python,
    binary_to_snowflake,
)


def test_basic():
    """Test hex and base64 formatting."""
    # Hex
    fmt = SnowflakeBinaryFormat("heX")
    assert fmt.format(b"") == ""
    assert fmt.format(b"\x00") == "00"
    assert fmt.format(b"\xAB\xCD\x12") == "ABCD12"
    assert fmt.format(b"\x00\xFF\x42\x01") == "00FF4201"

    # Base64
    fmt = SnowflakeBinaryFormat("BasE64")
    assert fmt.format(b"") == ""
    assert fmt.format(b"\x00") == "AA=="
    assert fmt.format(b"\xAB\xCD\x12") == "q80S"
    assert fmt.format(b"\x00\xFF\x42\x01") == "AP9CAQ=="


def test_binary_to_python():
    """Test conversion to Python data type."""
    assert binary_to_python("") == b""
    assert binary_to_python("00") == b"\x00"
    assert binary_to_python("ABCD12") == b"\xAB\xCD\x12"


def test_binary_to_snowflake():
    """Test conversion for passing to Snowflake."""
    assert binary_to_snowflake(b"") == b""
    assert binary_to_snowflake(b"\x00") == b"00"
    assert binary_to_snowflake(b"\xAB\xCD\x12") == b"ABCD12"
