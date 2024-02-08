import lz4.frame as lz4frame
import os
import sys
import pytest
from .helpers import get_frame_info_check


test_data = [
    (b''),
    (os.urandom(8 * 1024)),
    (b'0' * 8 * 1024),
    (bytearray(b'')),
    (bytearray(os.urandom(8 * 1024))),
    (os.urandom(128 * 1024)),
    (os.urandom(256 * 1024)),
    (os.urandom(512 * 1024)),
]
if sys.version_info > (2, 7):
    test_data += [
        (memoryview(b'')),
        (memoryview(os.urandom(8 * 1024)))
    ]


@pytest.fixture(
    params=test_data,
    ids=[
        'data' + str(i) for i in range(len(test_data))
    ]
)
def data(request):
    return request.param


def test_roundtrip_1(
        data,
        block_size,
        block_linked,
        content_checksum,
        block_checksum,
        compression_level,
        store_size):

    compressed = lz4frame.compress(
        data,
        store_size=store_size,
        compression_level=compression_level,
        block_size=block_size,
        block_linked=block_linked,
        content_checksum=content_checksum,
        block_checksum=block_checksum,
    )

    get_frame_info_check(
        compressed,
        len(data),
        store_size,
        block_size,
        block_linked,
        content_checksum,
        block_checksum,
    )
    decompressed, bytes_read = lz4frame.decompress(
        compressed, return_bytes_read=True)
    assert bytes_read == len(compressed)
    assert decompressed == data


def test_roundtrip_2(data,
                     block_size,
                     block_linked,
                     content_checksum,
                     block_checksum,
                     compression_level,
                     auto_flush,
                     store_size):

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
    compressed += lz4frame.compress_chunk(
        c_context,
        data
    )
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
    decompressed, bytes_read = lz4frame.decompress(
        compressed, return_bytes_read=True)
    assert bytes_read == len(compressed)
    assert decompressed == data
