import lz4.frame as lz4frame
import pytest
import os
import sys
from . helpers import (
    get_chunked,
    get_frame_info_check,
)


test_data = [
    (b'', 1, 1),
    (os.urandom(8 * 1024), 8, 1),
    (os.urandom(8 * 1024), 1, 8),
    (b'0' * 8 * 1024, 8, 1),
    (b'0' * 8 * 1024, 8, 1),
    (bytearray(b''), 1, 1),
    (bytearray(os.urandom(8 * 1024)), 8, 1),
]
if sys.version_info > (2, 7):
    test_data += [
        (memoryview(b''), 1, 1),
        (memoryview(os.urandom(8 * 1024)), 8, 1)
    ]


@pytest.fixture(
    params=test_data,
    ids=[
        'data' + str(i) for i in range(len(test_data))
    ]
)
def data(request):
    return request.param


def test_roundtrip_chunked(data, block_size, block_linked,
                           content_checksum, block_checksum,
                           compression_level,
                           auto_flush, store_size):

    data, c_chunks, d_chunks = data

    c_context = lz4frame.create_compression_context()

    kwargs = {}
    kwargs['compression_level'] = compression_level
    kwargs['block_size'] = block_size
    kwargs['block_linked'] = block_linked
    kwargs['content_checksum'] = content_checksum
    kwargs['block_checksum'] = block_checksum
    kwargs['auto_flush'] = auto_flush
    if store_size is True:
        kwargs['source_size'] = len(data)

    compressed = lz4frame.compress_begin(
        c_context,
        **kwargs
    )
    data_in = get_chunked(data, c_chunks)
    try:
        while True:
            compressed += lz4frame.compress_chunk(
                c_context,
                next(data_in)
            )
    except StopIteration:
        pass
    finally:
        del data_in

    compressed += lz4frame.compress_flush(c_context)

    get_frame_info_check(
        compressed,
        len(data),
        store_size,
        block_size,
        block_linked,
        content_checksum,
        block_checksum,
    )

    d_context = lz4frame.create_decompression_context()
    compressed_in = get_chunked(compressed, d_chunks)
    decompressed = b''
    bytes_read = 0
    eofs = []
    try:
        while True:
            d, b, e = lz4frame.decompress_chunk(
                d_context,
                next(compressed_in),
            )
            decompressed += d
            bytes_read += b
            eofs.append(e)

    except StopIteration:
        pass
    finally:
        del compressed_in

    assert bytes_read == len(compressed)
    assert decompressed == data
    assert eofs[-1] is True
    assert (True in eofs[:-2]) is False
