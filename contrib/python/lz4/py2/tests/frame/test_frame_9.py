import os
import lz4.frame


def test_issue_172_1():
    """Test reproducer for issue 172

    Issue 172 is a reported failure occurring on Windows 10 only. This bug was
    due to incorrect handling of Py_ssize_t types when doing comparisons and
    using them as a size when allocating memory.

    """
    input_data = 8 * os.urandom(1024)
    with lz4.frame.open('testfile_small', 'wb') as fp:
        bytes_written = fp.write(input_data)  # noqa: F841

    with lz4.frame.open('testfile_small', 'rb') as fp:
        data = fp.read(10)
        assert len(data) == 10


def test_issue_172_2():
    input_data = 9 * os.urandom(1024)
    with lz4.frame.open('testfile_small', 'w') as fp:
        bytes_written = fp.write(input_data)  # noqa: F841

    with lz4.frame.open('testfile_small', 'r') as fp:
        data = fp.read(10)
        assert len(data) == 10


def test_issue_172_3():
    input_data = 9 * os.urandom(1024)
    with lz4.frame.open('testfile_small', 'wb') as fp:
        bytes_written = fp.write(input_data)  # noqa: F841

    with lz4.frame.open('testfile_small', 'rb') as fp:
        data = fp.read(10)
        assert len(data) == 10

    with lz4.frame.open('testfile_small', 'rb') as fp:
        data = fp.read(16 * 1024 - 1)
        assert len(data) == 9 * 1024
        assert data == input_data
