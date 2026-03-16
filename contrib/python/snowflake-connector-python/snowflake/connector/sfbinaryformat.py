#!/usr/bin/env python
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

from base64 import b16decode, b16encode, standard_b64encode

from .errors import InternalError

# Converts a Snowflake binary value into a "bytes" object.
binary_to_python = b16decode


def binary_to_snowflake(binary_value) -> bytes | bytearray:
    """Encodes a "bytes" object for passing to Snowflake."""
    result = b16encode(binary_value)

    if isinstance(binary_value, bytearray):
        return bytearray(result)
    return result


class SnowflakeBinaryFormat:
    """Formats binary values ("bytes" objects) in hex or base64."""

    def __init__(self, name):
        name = name.upper()
        if name == "HEX":
            self._encode = b16encode
        elif name == "BASE64":
            self._encode = standard_b64encode
        else:
            raise InternalError(f"Unrecognized binary format {name}")

    def format(self, binary_value):
        """Formats a "bytes" object, returning a string."""
        return self._encode(binary_value).decode("ascii")
