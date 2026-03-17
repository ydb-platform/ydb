#!/usr/bin/env python
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

import collections.abc
import decimal
import html
import http.client
import os
import platform
import queue
import urllib.parse
import urllib.request
from typing import Any

from . import constants

IS_LINUX = platform.system() == "Linux"
IS_WINDOWS = platform.system() == "Windows"
IS_MACOS = platform.system() == "Darwin"

NUM_DATA_TYPES: tuple[type, ...] = ()
try:
    import numpy

    NUM_DATA_TYPES = (
        numpy.int8,
        numpy.int16,
        numpy.int32,
        numpy.int64,
        numpy.float16,
        numpy.float32,
        numpy.float64,
        numpy.uint8,
        numpy.uint16,
        numpy.uint32,
        numpy.uint64,
        numpy.bool_,
    )
except (ImportError, AttributeError):
    numpy = None

GET_CWD = os.getcwd
BASE_EXCEPTION_CLASS = Exception
TO_UNICODE = str
ITERATOR = collections.abc.Iterator
MAPPING = collections.abc.Mapping

urlsplit = urllib.parse.urlsplit
urlunsplit = urllib.parse.urlunsplit
parse_qs = urllib.parse.parse_qs
urlparse = urllib.parse.urlparse
urlunparse = urllib.parse.urlunparse

NUM_DATA_TYPES += (int, float, decimal.Decimal)


def PKCS5_UNPAD(v):
    return v[0 : -v[-1]]


def PKCS5_OFFSET(v):
    return v[-1]


def IS_BINARY(v):
    return isinstance(v, (bytes, bytearray))


METHOD_NOT_ALLOWED = http.client.METHOD_NOT_ALLOWED
BAD_GATEWAY = http.client.BAD_GATEWAY
BAD_REQUEST = http.client.BAD_REQUEST
REQUEST_TIMEOUT = http.client.REQUEST_TIMEOUT
SERVICE_UNAVAILABLE = http.client.SERVICE_UNAVAILABLE
GATEWAY_TIMEOUT = http.client.GATEWAY_TIMEOUT
FORBIDDEN = http.client.FORBIDDEN
UNAUTHORIZED = http.client.UNAUTHORIZED
INTERNAL_SERVER_ERROR = http.client.INTERNAL_SERVER_ERROR
IncompleteRead = http.client.IncompleteRead
OK = http.client.OK
BadStatusLine = http.client.BadStatusLine

urlencode = urllib.parse.urlencode
unquote = urllib.parse.unquote
quote = urllib.parse.quote
unescape = html.unescape

EmptyQueue = queue.Empty
Queue = queue.Queue


def IS_BYTES(v: Any) -> bool:
    return isinstance(v, bytes)


def IS_UNICODE(v: Any) -> bool:
    return isinstance(v, str)


def IS_NUMERIC(v: Any) -> bool:
    return isinstance(v, NUM_DATA_TYPES)


IS_STR = IS_UNICODE


def PKCS5_PAD(value: bytes, block_size: int) -> bytes:
    return b"".join(
        [
            value,
            (block_size - len(value) % block_size)
            * chr(block_size - len(value) % block_size).encode(constants.UTF8),
        ]
    )


def PRINT(msg: str) -> None:
    print(msg)


def INPUT(prompt: str) -> str:
    return input(prompt)


def quote_url_piece(piece: str) -> str:
    """Helper function to urlencode a string and turn it into bytes."""
    return quote(piece)
