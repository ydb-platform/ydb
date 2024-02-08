import lz4.frame as lz4frame
import pytest
import os

test_data = [
    (os.urandom(32) * 256),
]


@pytest.fixture(
    params=test_data,
    ids=[
        'data' + str(i) for i in range(len(test_data))
    ]
)
def data(request):
    return request.param


def test_roundtrip_multiframe_1(data):
    nframes = 4

    compressed = b''
    for _ in range(nframes):
        compressed += lz4frame.compress(data)

    decompressed = b''
    for _ in range(nframes):
        decompressed += lz4frame.decompress(compressed)

    assert len(decompressed) == nframes * len(data)
    assert data * nframes == decompressed


def test_roundtrip_multiframe_2(data):
    nframes = 4

    compressed = b''
    ctx = lz4frame.create_compression_context()
    for _ in range(nframes):
        compressed += lz4frame.compress_begin(ctx)
        compressed += lz4frame.compress_chunk(ctx, data)
        compressed += lz4frame.compress_flush(ctx)

    decompressed = b''
    for _ in range(nframes):
        decompressed += lz4frame.decompress(compressed)

    assert len(decompressed) == nframes * len(data)
    assert data * nframes == decompressed


def test_roundtrip_multiframe_3(data):
    nframes = 4

    compressed = b''
    ctx = lz4frame.create_compression_context()
    for _ in range(nframes):
        compressed += lz4frame.compress_begin(ctx)
        compressed += lz4frame.compress_chunk(ctx, data)
        compressed += lz4frame.compress_flush(ctx)

    decompressed = b''
    ctx = lz4frame.create_decompression_context()
    for _ in range(nframes):
        d, bytes_read, eof = lz4frame.decompress_chunk(ctx, compressed)
        decompressed += d
        assert eof is True
        assert bytes_read == len(compressed) // nframes

    assert len(decompressed) == nframes * len(data)
    assert data * nframes == decompressed


def test_roundtrip_multiframe_4(data):
    nframes = 4

    compressed = b''
    with lz4frame.LZ4FrameCompressor() as compressor:
        for _ in range(nframes):
            compressed += compressor.begin()
            compressed += compressor.compress(data)
            compressed += compressor.flush()

    decompressed = b''
    with lz4frame.LZ4FrameDecompressor() as decompressor:
        for i in range(nframes):
            if i == 0:
                d = compressed
            else:
                d = decompressor.unused_data
            decompressed += decompressor.decompress(d)
            assert decompressor.eof is True
            assert decompressor.needs_input is True
            if i == nframes - 1:
                assert decompressor.unused_data is None
            else:
                assert len(decompressor.unused_data) == len(
                    compressed) * (nframes - i - 1) / nframes

    assert len(decompressed) == nframes * len(data)
    assert data * nframes == decompressed
