# Copyright (c) 2018-2024 NCC Group Plc
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import zlib

from blackboxprotobuf.lib.exceptions import BlackboxProtobufException

import six

if six.PY3:
    from typing import Tuple


def is_gzip(buf):
    # type: (bytes) -> bool
    return buf.startswith(bytearray([0x1F, 0x8B, 0x08]))


def decode_gzip(buf):
    # type: (bytes) -> Tuple[bytes, str]
    if buf.startswith(bytearray([0x1F, 0x8B, 0x08])):
        decompressor = zlib.decompressobj(31)
        return decompressor.decompress(buf), "gzip"
    else:
        raise BlackboxProtobufException(
            "Cannot decode as gzip: magic bytes don't match"
        )


def encode_gzip(buf):
    # type: (bytes | list[bytes]) -> bytes
    if isinstance(buf, list):
        raise BlackboxProtobufException(
            "Cannot encode as gzip: multiple buffers are not supported"
        )
    compressor = zlib.compressobj(-1, zlib.DEFLATED, 31)
    return compressor.compress(buf) + compressor.flush()
