#!/usr/bin/env python
#
# Copyright (c) 2011, Andres Moreira <andres@andresmoreira.com>
#               2011, Felipe Cruz <felipecruz@loogica.net>
#               2012, JT Olds <jt@spacemonkey.com>
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#     * Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * Neither the name of the authors nor the
#       names of its contributors may be used to endorse or promote products
#       derived from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL ANDRES MOREIRA BE LIABLE FOR ANY DIRECT,
# INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#

"""python-snappy

Python library for the snappy compression library from Google.
Expected usage like:

    import snappy

    compressed = snappy.compress("some data")
    assert "some data" == snappy.uncompress(compressed)

"""
from __future__ import absolute_import

import sys
import struct

try:
    from ._snappy import UncompressError, compress, decompress, \
                         isValidCompressed, uncompress, _crc32c
except ImportError:
    from .snappy_cffi import UncompressError, compress, decompress, \
                             isValidCompressed, uncompress, _crc32c

_CHUNK_MAX = 65536
_STREAM_TO_STREAM_BLOCK_SIZE = _CHUNK_MAX
_STREAM_IDENTIFIER = b"sNaPpY"
_COMPRESSED_CHUNK = 0x00
_UNCOMPRESSED_CHUNK = 0x01
_IDENTIFIER_CHUNK = 0xff
_RESERVED_UNSKIPPABLE = (0x02, 0x80)  # chunk ranges are [inclusive, exclusive)
_RESERVED_SKIPPABLE = (0x80, 0xff)

# the minimum percent of bytes compression must save to be enabled in automatic
# mode
_COMPRESSION_THRESHOLD = .125

def _masked_crc32c(data):
    # see the framing format specification
    crc = _crc32c(data)
    return (((crc >> 15) | (crc << 17)) + 0xa282ead8) & 0xffffffff

_compress = compress
_uncompress = uncompress


py3k = False
if sys.hexversion > 0x03000000:
    unicode = str
    py3k = True

def compress(data, encoding='utf-8'):
    if isinstance(data, unicode):
        data = data.encode(encoding)

    return _compress(data)

def uncompress(data, decoding=None):
    if isinstance(data, unicode):
        raise UncompressError("It's only possible to uncompress bytes")
    if decoding:
        return _uncompress(data).decode(decoding)
    return _uncompress(data)

decompress = uncompress


class StreamCompressor(object):

    """This class implements the compressor-side of the proposed Snappy framing
    format, found at

        http://code.google.com/p/snappy/source/browse/trunk/framing_format.txt
            ?spec=svn68&r=71

    This class matches the interface found for the zlib module's compression
    objects (see zlib.compressobj), but also provides some additions, such as
    the snappy framing format's ability to intersperse uncompressed data.

    Keep in mind that this compressor object does no buffering for you to
    appropriately size chunks. Every call to StreamCompressor.compress results
    in a unique call to the underlying snappy compression method.
    """

    __slots__ = ["_header_chunk_written"]

    def __init__(self):
        self._header_chunk_written = False

    def add_chunk(self, data, compress=None):
        """Add a chunk containing 'data', returning a string that is framed and
        (optionally, default) compressed. This data should be concatenated to
        the tail end of an existing Snappy stream. In the absence of any
        internal buffering, no data is left in any internal buffers, and so
        unlike zlib.compress, this method returns everything.

        If compress is None, compression is determined automatically based on
        snappy's performance. If compress == True, compression always happens,
        and if compress == False, compression never happens.
        """
        out = bytearray()
        if not self._header_chunk_written:
            self._header_chunk_written = True
            out.extend(struct.pack("<L", _IDENTIFIER_CHUNK +
                                      (len(_STREAM_IDENTIFIER) << 8)))
            out.extend(_STREAM_IDENTIFIER)
        for i in range(0, len(data), _CHUNK_MAX):
            chunk = data[i:i + _CHUNK_MAX]
            crc = _masked_crc32c(chunk)
            if compress is None:
                compressed_chunk = _compress(chunk)
                if (len(compressed_chunk) <=
                        (1 - _COMPRESSION_THRESHOLD) * len(chunk)):
                    chunk = compressed_chunk
                    chunk_type = _COMPRESSED_CHUNK
                else:
                    chunk_type = _UNCOMPRESSED_CHUNK
                compressed_chunk = None
            elif compress:
                chunk = _compress(chunk)
                chunk_type = _COMPRESSED_CHUNK
            else:
                chunk_type = _UNCOMPRESSED_CHUNK
            out.extend(struct.pack("<LL", chunk_type + ((len(chunk) + 4) << 8),
                                   crc))
            out.extend(chunk)
        return bytes(out)

    def compress(self, data):
        """This method is simply an alias for compatibility with zlib
        compressobj's compress method.
        """
        return self.add_chunk(data)

    def flush(self, mode=None):
        """This method does nothing and only exists for compatibility with
        the zlib compressobj
        """
        pass

    def copy(self):
        """This method exists for compatibility with the zlib compressobj.
        """
        copy = StreamCompressor()
        copy._header_chunk_written = self._header_chunk_written
        return copy


class StreamDecompressor(object):

    """This class implements the decompressor-side of the proposed Snappy
    framing format, found at

        http://code.google.com/p/snappy/source/browse/trunk/framing_format.txt
            ?spec=svn68&r=71

    This class matches a subset of the interface found for the zlib module's
    decompression objects (see zlib.decompressobj). Specifically, it currently
    implements the decompress method without the max_length option, the flush
    method without the length option, and the copy method.
    """

    __slots__ = ["_buf", "_header_found"]

    def __init__(self):
        self._buf = bytearray()
        self._header_found = False

    @staticmethod
    def check_format(data):
        """Checks that the given data starts with snappy framing format
        stream identifier.
        Raises UncompressError if it doesn't start with the identifier.
        :return: None
        """
        if len(data) < 6:
            raise UncompressError("Too short data length")
        chunk_type = struct.unpack("<L", data[:4])[0]
        size = (chunk_type >> 8)
        chunk_type &= 0xff
        if (chunk_type != _IDENTIFIER_CHUNK or
                size != len(_STREAM_IDENTIFIER)):
            raise UncompressError("stream missing snappy identifier")
        chunk = data[4:4 + size]
        if chunk != _STREAM_IDENTIFIER:
            raise UncompressError("stream has invalid snappy identifier")

    def decompress(self, data):
        """Decompress 'data', returning a string containing the uncompressed
        data corresponding to at least part of the data in string. This data
        should be concatenated to the output produced by any preceding calls to
        the decompress() method. Some of the input data may be preserved in
        internal buffers for later processing.
        """
        self._buf.extend(data)
        uncompressed = bytearray()
        while True:
            if len(self._buf) < 4:
                return bytes(uncompressed)
            chunk_type = struct.unpack("<L", self._buf[:4])[0]
            size = (chunk_type >> 8)
            chunk_type &= 0xff
            if not self._header_found:
                if (chunk_type != _IDENTIFIER_CHUNK or
                        size != len(_STREAM_IDENTIFIER)):
                    raise UncompressError("stream missing snappy identifier")
                self._header_found = True
            if (_RESERVED_UNSKIPPABLE[0] <= chunk_type and
                    chunk_type < _RESERVED_UNSKIPPABLE[1]):
                raise UncompressError(
                    "stream received unskippable but unknown chunk")
            if len(self._buf) < 4 + size:
                return bytes(uncompressed)
            chunk, self._buf = self._buf[4:4 + size], self._buf[4 + size:]
            if chunk_type == _IDENTIFIER_CHUNK:
                if chunk != _STREAM_IDENTIFIER:
                    raise UncompressError(
                        "stream has invalid snappy identifier")
                continue
            if (_RESERVED_SKIPPABLE[0] <= chunk_type and
                    chunk_type < _RESERVED_SKIPPABLE[1]):
                continue
            assert chunk_type in (_COMPRESSED_CHUNK, _UNCOMPRESSED_CHUNK)
            crc, chunk = chunk[:4], chunk[4:]
            if chunk_type == _COMPRESSED_CHUNK:
                chunk = _uncompress(chunk)
            if struct.pack("<L", _masked_crc32c(chunk)) != crc:
                raise UncompressError("crc mismatch")
            uncompressed += chunk

    def flush(self):
        """All pending input is processed, and a string containing the
        remaining uncompressed output is returned. After calling flush(), the
        decompress() method cannot be called again; the only realistic action
        is to delete the object.
        """
        if self._buf != b"":
            raise UncompressError("chunk truncated")
        return b""

    def copy(self):
        """Returns a copy of the decompression object. This can be used to save
        the state of the decompressor midway through the data stream in order
        to speed up random seeks into the stream at a future point.
        """
        copy = StreamDecompressor()
        copy._buf, copy._header_found = bytearray(self._buf), self._header_found
        return copy


def stream_compress(src,
                    dst,
                    blocksize=_STREAM_TO_STREAM_BLOCK_SIZE,
                    compressor_cls=StreamCompressor):
    """Takes an incoming file-like object and an outgoing file-like object,
    reads data from src, compresses it, and writes it to dst. 'src' should
    support the read method, and 'dst' should support the write method.

    The default blocksize is good for almost every scenario.
    """
    compressor = compressor_cls()
    while True:
        buf = src.read(blocksize)
        if not buf: break
        buf = compressor.add_chunk(buf)
        if buf: dst.write(buf)


def stream_decompress(src,
                      dst,
                      blocksize=_STREAM_TO_STREAM_BLOCK_SIZE,
                      decompressor_cls=StreamDecompressor,
                      start_chunk=None):
    """Takes an incoming file-like object and an outgoing file-like object,
    reads data from src, decompresses it, and writes it to dst. 'src' should
    support the read method, and 'dst' should support the write method.

    The default blocksize is good for almost every scenario.
    :param decompressor_cls: class that implements `decompress` method like
        StreamDecompressor in the module
    :param start_chunk: start block of data that have already been read from
        the input stream (to detect the format, for example)
    """
    decompressor = decompressor_cls()
    while True:
        if start_chunk:
            buf = start_chunk
            start_chunk = None
        else:
            buf = src.read(blocksize)
            if not buf: break
        buf = decompressor.decompress(buf)
        if buf: dst.write(buf)
    decompressor.flush()  # makes sure the stream ended well


def check_format(fin=None, chunk=None,
                 blocksize=_STREAM_TO_STREAM_BLOCK_SIZE,
                 decompressor_cls=StreamDecompressor):
    ok = True
    if chunk is None:
        chunk = fin.read(blocksize)
        if not chunk:
            raise UncompressError("Empty input stream")
    try:
        decompressor_cls.check_format(chunk)
    except UncompressError as err:
        ok = False
    return ok, chunk
