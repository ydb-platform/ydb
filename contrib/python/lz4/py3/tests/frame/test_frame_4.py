import lz4.frame as lz4frame
import os
import pytest
from . helpers import (
    get_frame_info_check,
    get_chunked,
)

test_data = [
    b'',
    (128 * (32 * os.urandom(32))),
    (256 * (32 * os.urandom(32))),
    (512 * (32 * os.urandom(32))),
    (1024 * (32 * os.urandom(32))),
]


@pytest.fixture(
    params=test_data,
    ids=[
        'data' + str(i) for i in range(len(test_data))
    ]
)
def data(request):
    return request.param


@pytest.fixture(
    params=[
        (True),
        (False)
    ]
)
def reset(request):
    return request.param


@pytest.fixture(
    params=[
        (1),
        (8)
    ]
)
def chunks(request):
    return request.param


def test_roundtrip_LZ4FrameCompressor(
        data,
        chunks,
        block_size,
        block_linked,
        reset,
        store_size,
        block_checksum,
        content_checksum):

    with lz4frame.LZ4FrameCompressor(
            block_size=block_size,
            block_linked=block_linked,
            content_checksum=content_checksum,
            block_checksum=block_checksum,
    ) as compressor:
        def do_compress():
            if store_size is True:
                compressed = compressor.begin(source_size=len(data))
            else:
                compressed = compressor.begin()

            for chunk in get_chunked(data, chunks):
                compressed += compressor.compress(chunk)

            compressed += compressor.flush()
            return compressed

        compressed = do_compress()

        if reset is True:
            compressor.reset()
            compressed = do_compress()

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
    assert data == decompressed
    assert bytes_read == len(compressed)


def test_roundtrip_LZ4FrameCompressor_LZ4FrameDecompressor(
        data,
        chunks,
        block_size,
        block_linked,
        reset,
        store_size,
        block_checksum,
        content_checksum):

    with lz4frame.LZ4FrameCompressor(
            block_size=block_size,
            block_linked=block_linked,
            content_checksum=content_checksum,
            block_checksum=block_checksum,
    ) as compressor:
        def do_compress():
            if store_size is True:
                compressed = compressor.begin(source_size=len(data))
            else:
                compressed = compressor.begin()

            for chunk in get_chunked(data, chunks):
                compressed += compressor.compress(chunk)

            compressed += compressor.flush()
            return compressed

        compressed = do_compress()

        if reset is True:
            compressor.reset()
            compressed = do_compress()

    get_frame_info_check(
        compressed,
        len(data),
        store_size,
        block_size,
        block_linked,
        content_checksum,
        block_checksum,
    )

    with lz4frame.LZ4FrameDecompressor() as decompressor:
        decompressed = b''
        for chunk in get_chunked(compressed, chunks):
            b = decompressor.decompress(chunk)
            decompressed += b

    assert data == decompressed
