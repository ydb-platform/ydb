"""The module implements compression/decompression with snappy using
Hadoop snappy format: https://github.com/kubo/snzip#hadoop-snappy-format

Expected usage like:

    import snappy

    src = 'uncompressed'
    dst = 'compressed'
    dst2 = 'decompressed'

    with open(src, 'rb') as fin, open(dst, 'wb') as fout:
        snappy.hadoop_stream_compress(src, dst)

    with open(dst, 'rb') as fin, open(dst2, 'wb') as fout:
        snappy.hadoop_stream_decompress(fin, fout)

    with open(src, 'rb') as fin1, open(dst2, 'rb') as fin2:
        assert fin1.read() == fin2.read()

"""
from __future__ import absolute_import

import struct

from .snappy import (
    _compress, _uncompress,
    stream_compress as _stream_compress,
    stream_decompress as _stream_decompress,
    check_format as _check_format,
    UncompressError,
    _CHUNK_MAX)


SNAPPY_BUFFER_SIZE_DEFAULT = 256 * 1024
_STREAM_TO_STREAM_BLOCK_SIZE = _CHUNK_MAX

_INT_SIZE = 4


def pack_int(num):
    big_endian_uint = struct.pack('>I', num)
    return big_endian_uint


def unpack_int(data):
    return struct.unpack('>I', data)[0]


class StreamCompressor(object):

    """This class implements the compressor-side of the hadoop snappy
    format, taken from https://github.com/kubo/snzip#hadoop-snappy-format

    Keep in mind that this compressor object does no buffering for you to
    appropriately size chunks. Every call to StreamCompressor.compress results
    in a unique call to the underlying snappy compression method.
    """

    def __init__(self):
        pass

    def add_chunk(self, data):
        """Add a chunk containing 'data', returning a string that is
        compressed. This data should be concatenated to
        the tail end of an existing Snappy stream. In the absence of any
        internal buffering, no data is left in any internal buffers, and so
        unlike zlib.compress, this method returns everything.
        """
        out = []
        uncompressed_length = len(data)
        out.append(pack_int(uncompressed_length))
        compressed_chunk = _compress(data)
        compressed_length = len(compressed_chunk)
        out.append(pack_int(compressed_length))
        out.append(compressed_chunk)
        return b"".join(out)

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
        return StreamCompressor()


class StreamDecompressor(object):

    """This class implements the decompressor-side of the hadoop snappy
    format.

    This class matches a subset of the interface found for the zlib module's
    decompression objects (see zlib.decompressobj). Specifically, it currently
    implements the decompress method without the max_length option, the flush
    method without the length option, and the copy method.
    """

    __slots__ = ["_buf", "_block_length", "_uncompressed_length"]

    def __init__(self):
        self._buf = b""
        # current block length
        self._block_length = 0
        # total uncompressed data length of the current block
        self._uncompressed_length = 0

    @staticmethod
    def check_format(data):
        """Just checks that first two integers (big endian four-bytes int)
        in the given data block comply to: first int >= second int.
        This is a simple assumption that we have in the data a start of a
        block for hadoop snappy format. It should contain uncompressed block
        length as the first integer, and compressed subblock length as the
        second integer.
        Raises UncompressError if the condition is not fulfilled.
        :return: None
        """
        int_size = _INT_SIZE
        if len(data) < int_size * 2:
            raise UncompressError("Too short data length")
        # We cant actually be sure abot the format here.
        # Assumption that compressed data length is less than uncompressed
        # is not true in general.
        # So, just don't check anything
        return

    def decompress(self, data):
        """Decompress 'data', returning a string containing the uncompressed
        data corresponding to at least part of the data in string. This data
        should be concatenated to the output produced by any preceding calls to
        the decompress() method. Some of the input data may be preserved in
        internal buffers for later processing.
        """
        int_size = _INT_SIZE
        self._buf += data
        uncompressed = []
        while True:
            if len(self._buf) < int_size:
                return b"".join(uncompressed)
            next_start = 0
            if not self._block_length:
                self._block_length = unpack_int(self._buf[:int_size])
                self._buf = self._buf[int_size:]
                if len(self._buf) < int_size:
                    return b"".join(uncompressed)
            compressed_length = unpack_int(
                self._buf[next_start:next_start + int_size]
            )
            next_start += int_size
            if len(self._buf) < compressed_length + next_start:
                return b"".join(uncompressed)
            chunk = self._buf[
                next_start:next_start + compressed_length
            ]
            self._buf = self._buf[next_start + compressed_length:]
            uncompressed_chunk = _uncompress(chunk)
            self._uncompressed_length += len(uncompressed_chunk)
            uncompressed.append(uncompressed_chunk)
            if self._uncompressed_length == self._block_length:
                # Here we have uncompressed all subblocks of the current block
                self._uncompressed_length = 0
                self._block_length = 0
                continue

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
        copy._buf = self._buf
        copy._block_length = self._block_length
        copy._uncompressed_length = self._uncompressed_length
        return copy


def stream_compress(src, dst, blocksize=SNAPPY_BUFFER_SIZE_DEFAULT):
    return _stream_compress(
        src, dst, blocksize=blocksize, compressor_cls=StreamCompressor
    )


def stream_decompress(src, dst, blocksize=_STREAM_TO_STREAM_BLOCK_SIZE,
                      start_chunk=None):
    return _stream_decompress(
        src, dst, blocksize=blocksize,
        decompressor_cls=StreamDecompressor,
        start_chunk=start_chunk
    )


def check_format(fin=None, chunk=None, blocksize=_STREAM_TO_STREAM_BLOCK_SIZE):
    return _check_format(
        fin=fin, chunk=chunk, blocksize=blocksize,
        decompressor_cls=StreamDecompressor
    )
