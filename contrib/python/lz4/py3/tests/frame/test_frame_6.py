import os
import pytest
import threading
import lz4.frame as lz4frame

test_data = [
    b'',
    (128 * (32 * os.urandom(32))),
    (5 * 128 * os.urandom(1024)),
]


@pytest.fixture(
    params=test_data,
    ids=[
        'data' + str(i) for i in range(len(test_data))
    ]
)
def data(request):
    return request.param


compression_levels = [
    (lz4frame.COMPRESSIONLEVEL_MIN),
    # (lz4frame.COMPRESSIONLEVEL_MINHC),
    # (lz4frame.COMPRESSIONLEVEL_MAX),
]


@pytest.fixture(
    params=compression_levels
)
def compression_level(request):
    return request.param


def test_lz4frame_open_write(tmp_path, data):
    thread_id = threading.get_native_id()
    with lz4frame.open(tmp_path / f'testfile_{thread_id}', mode='wb') as fp:
        fp.write(data)


def test_lz4frame_open_write_read_defaults(tmp_path, data):
    thread_id = threading.get_native_id()
    with lz4frame.open(tmp_path / f'testfile_{thread_id}', mode='wb') as fp:
        fp.write(data)
    with lz4frame.open(tmp_path / f'testfile_{thread_id}', mode='r') as fp:
        data_out = fp.read()
    assert data_out == data


def test_lz4frame_open_write_read_text(tmp_path):
    data = u'This is a test string'
    thread_id = threading.get_native_id()
    with lz4frame.open(tmp_path / f'testfile_{thread_id}', mode='wt') as fp:
        fp.write(data)
    with lz4frame.open(tmp_path / f'testfile_{thread_id}', mode='rt') as fp:
        data_out = fp.read()
    assert data_out == data


def test_lz4frame_open_write_read_text_iter(tmp_path):
    data = u'This is a test string'
    thread_id = threading.get_native_id()
    with lz4frame.open(tmp_path / f'testfile_{thread_id}', mode='wt') as fp:
        fp.write(data)
    data_out = ''
    with lz4frame.open(tmp_path / f'testfile_{thread_id}', mode='rt') as fp:
        for line in fp:
            data_out += line
    assert data_out == data


def test_lz4frame_open_write_read(
        tmp_path,
        data,
        compression_level,
        block_linked,
        block_checksum,
        block_size,
        content_checksum,
        auto_flush,
        store_size,
        return_bytearray):

    kwargs = {}

    if store_size is True:
        kwargs['source_size'] = len(data)

    kwargs['compression_level'] = compression_level
    kwargs['block_size'] = block_size
    kwargs['block_linked'] = block_linked
    kwargs['content_checksum'] = content_checksum
    kwargs['block_checksum'] = block_checksum
    kwargs['auto_flush'] = auto_flush
    kwargs['return_bytearray'] = return_bytearray
    kwargs['mode'] = 'wb'

    thread_id = threading.get_native_id()
    with lz4frame.open(tmp_path / f'testfile_{thread_id}', **kwargs) as fp:
        fp.write(data)

    with lz4frame.open(tmp_path / f'testfile_{thread_id}', mode='r') as fp:
        data_out = fp.read()

    assert data_out == data


def test_lz4frame_flush(tmp_path):
    data_1 = b"This is a..."
    data_2 = b" test string!"
    thread_id = threading.get_native_id()

    with lz4frame.open(tmp_path / f"testfile_{thread_id}", mode="w") as fp_write:
        fp_write.write(data_1)
        fp_write.flush()

        fp_write.write(data_2)

        with lz4frame.open(tmp_path / f"testfile_{thread_id}", mode="r") as fp_read:
            assert fp_read.read() == data_1

        fp_write.flush()

        with lz4frame.open(tmp_path / f"testfile_{thread_id}", mode="r") as fp_read:
            assert fp_read.read() == data_1 + data_2
