"""Consts and function to handle target format.
ALL_SUPPORTED_FORMATS - list of supported formats
get_decompress_function - returns stream decompress function for a current
    format (specified or autodetected)
get_compress_function - returns compress function for a current format
    (specified or default)
"""
from __future__ import absolute_import

from .snappy import (
    stream_compress, stream_decompress, check_format, UncompressError)
from .hadoop_snappy import (
    stream_compress as hadoop_stream_compress,
    stream_decompress as hadoop_stream_decompress,
    check_format as hadoop_check_format)


FRAMING_FORMAT = 'framing'

HADOOP_FORMAT = 'hadoop_snappy'

# Means format auto detection.
# For compression will be used framing format.
# In case of decompression will try to detect a format from the input stream
# header.
FORMAT_AUTO = 'auto'

DEFAULT_FORMAT = FORMAT_AUTO

ALL_SUPPORTED_FORMATS = [FRAMING_FORMAT, HADOOP_FORMAT, FORMAT_AUTO]

_COMPRESS_METHODS = {
    FRAMING_FORMAT: stream_compress,
    HADOOP_FORMAT: hadoop_stream_compress,
}

_DECOMPRESS_METHODS = {
    FRAMING_FORMAT: stream_decompress,
    HADOOP_FORMAT: hadoop_stream_decompress,
}

# We will use framing format as the default to compression.
# And for decompression, if it's not defined explicitly, we will try to
# guess the format from the file header.
_DEFAULT_COMPRESS_FORMAT = FRAMING_FORMAT

# The tuple contains an ordered sequence of a format checking function and
# a format-specific decompression function.
# Framing format has it's header, that may be recognized.
# Hadoop snappy format hasn't any special headers, it contains only
# uncompressed block length integer and length of compressed subblock.
# So we first check framing format and if it is not the case, then
# check for snappy format.
_DECOMPRESS_FORMAT_FUNCS = (
    (check_format, stream_decompress),
    (hadoop_check_format, hadoop_stream_decompress),
)


def guess_format_by_header(fin):
    """Tries to guess a compression format for the given input file by it's
    header.
    :return: tuple of decompression method and a chunk that was taken from the
        input for format detection.
    """
    chunk = None
    for check_method, decompress_func in _DECOMPRESS_FORMAT_FUNCS:
        ok, chunk = check_method(fin=fin, chunk=chunk)
        if not ok:
            continue
        return decompress_func, chunk
    raise UncompressError("Can't detect archive format")


def get_decompress_function(specified_format, fin):
    if specified_format == FORMAT_AUTO:
        decompress_func, read_chunk = guess_format_by_header(fin)
        return decompress_func, read_chunk
    return _DECOMPRESS_METHODS[specified_format], None


def get_compress_function(specified_format):
    if specified_format == FORMAT_AUTO:
        return _COMPRESS_METHODS[_DEFAULT_COMPRESS_FORMAT]
    return _COMPRESS_METHODS[specified_format]
