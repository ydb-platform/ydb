import array
import os
import io
import pickle
import sys
import threading
import lz4.frame
import pytest


def test_issue_172_1(tmp_path):
    """Test reproducer for issue 172

    Issue 172 is a reported failure occurring on Windows 10 only. This bug was
    due to incorrect handling of Py_ssize_t types when doing comparisons and
    using them as a size when allocating memory.

    """
    input_data = 8 * os.urandom(1024)
    thread_id = threading.get_native_id()

    with lz4.frame.open(tmp_path / f'testfile_small_{thread_id}', 'wb') as fp:
        bytes_written = fp.write(input_data)  # noqa: F841

    with lz4.frame.open(tmp_path / f'testfile_small_{thread_id}', 'rb') as fp:
        data = fp.read(10)
        assert len(data) == 10


def test_issue_172_2(tmp_path):
    input_data = 9 * os.urandom(1024)
    thread_id = threading.get_native_id()
    with lz4.frame.open(tmp_path / f'testfile_small_{thread_id}', 'w') as fp:
        bytes_written = fp.write(input_data)  # noqa: F841

    with lz4.frame.open(tmp_path / f'testfile_small_{thread_id}', 'r') as fp:
        data = fp.read(10)
        assert len(data) == 10


def test_issue_172_3(tmp_path):
    input_data = 9 * os.urandom(1024)
    thread_id = threading.get_native_id()
    with lz4.frame.open(tmp_path / f'testfile_small_{thread_id}', 'wb') as fp:
        bytes_written = fp.write(input_data)  # noqa: F841

    with lz4.frame.open(tmp_path / f'testfile_small_{thread_id}', 'rb') as fp:
        data = fp.read(10)
        assert len(data) == 10

    with lz4.frame.open(tmp_path / f'testfile_small_{thread_id}', 'rb') as fp:
        data = fp.read(16 * 1024 - 1)
        assert len(data) == 9 * 1024
        assert data == input_data


def test_issue_227_1():
    q = array.array('Q', [1, 2, 3, 4, 5])
    LENGTH = len(q) * q.itemsize

    with lz4.frame.open(io.BytesIO(), 'w') as f:
        assert f.write(q) == LENGTH
        assert f.tell() == LENGTH


@pytest.mark.skipif(
    sys.version_info < (3, 8),
    reason="PickleBuffer only available in Python 3.8 or greater"
)
def test_issue_227_2():
    q = array.array('Q', [1, 2, 3, 4, 5])

    c = lz4.frame.compress(q)
    d = lz4.frame.LZ4FrameDecompressor().decompress(pickle.PickleBuffer(c))

    assert memoryview(q).tobytes() == d
