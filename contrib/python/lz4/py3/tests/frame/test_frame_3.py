import lz4.frame as lz4frame
import pytest
import os
import struct

test_data = [
    (os.urandom(256 * 1024)),
]


@pytest.fixture(
    params=test_data,
    ids=[
        'data' + str(i) for i in range(len(test_data))
    ]
)
def data(request):
    return request.param


def test_decompress_truncated(data):
    compressed = lz4frame.compress(data)

    message = r'^LZ4F_getFrameInfo failed with code: ERROR_frameHeader_incomplete'
    with pytest.raises(RuntimeError, match=message):
        lz4frame.decompress(compressed[:6])

    for i in range(16, len(compressed) - 1, 5):  # 15 is the max size of the header
        message = r'^Frame incomplete. LZ4F_decompress returned:'
        try:
            lz4frame.decompress(compressed[:i])
        except RuntimeError as r:
            print(r)
        with pytest.raises(RuntimeError, match=message):
            lz4frame.decompress(compressed[:i])


def test_content_checksum_failure(data):
    compressed = lz4frame.compress(data, content_checksum=True)
    message = r'^LZ4F_decompress failed with code: ERROR_contentChecksum_invalid$'
    with pytest.raises(RuntimeError, match=message):
        last = struct.unpack('B', compressed[-1:])[0]
        lz4frame.decompress(compressed[:-1] + struct.pack('B', last ^ 0x42))


def test_block_checksum_failure(data):
    compressed = lz4frame.compress(
        data,
        content_checksum=True,
        block_checksum=True,
        return_bytearray=True,
    )
    message = r'^LZ4F_decompress failed with code: ERROR_blockChecksum_invalid$'
    if len(compressed) > 32:
        with pytest.raises(RuntimeError, match=message):
            compressed[22] = compressed[22] ^ 0x42
            lz4frame.decompress(compressed)
