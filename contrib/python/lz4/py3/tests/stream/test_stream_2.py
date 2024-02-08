import pytest
import sys
import lz4.stream
import psutil
import os


# This test requires allocating a big lump of memory. In order to
# avoid a massive memory allocation during byte compilation, we have
# to declare a variable for the size of the buffer we're going to
# create outside the scope of the function below. See:
# https://bugs.python.org/issue21074

_4GB = 0xffffffff  # actually 4GB - 1B, the maximum size on 4 bytes.

# This test will be killed on Travis due to the 3GB memory limit
# there. Unfortunately psutil reports the host memory, not the memory
# available to the container, and so can't be used to detect available
# memory, so instead, as an ugly hack for detecting we're on Travis we
# check for the TRAVIS environment variable being set. This is quite
# fragile.

if os.environ.get('TRAVIS') is not None or sys.maxsize < _4GB or \
   psutil.virtual_memory().available < _4GB:
    huge = None
else:
    try:
        huge = b'\0' * _4GB
    except (MemoryError, OverflowError):
        huge = None


@pytest.mark.skipif(
    os.environ.get('TRAVIS') is not None,
    reason='Skipping test on Travis due to insufficient memory'
)
@pytest.mark.skipif(
    sys.maxsize < _4GB,
    reason='Py_ssize_t too small for this test'
)
@pytest.mark.skipif(
    psutil.virtual_memory().available < _4GB or huge is None,
    reason='Insufficient system memory for this test'
)
def test_huge_1():
    data = b''
    kwargs = {
        'strategy': "double_buffer",
        'buffer_size': lz4.stream.LZ4_MAX_INPUT_SIZE,
        'store_comp_size': 4,
        'dictionary': huge,
    }

    if psutil.virtual_memory().available < 3 * kwargs['buffer_size']:
        # The internal LZ4 context will request at least 3 times buffer_size
        # as memory (2 buffer_size for the double-buffer, and 1.x buffer_size
        # for the output buffer)
        pytest.skip('Insufficient system memory for this test')

    # Triggering overflow error
    message = r'^Dictionary too large for LZ4 API$'

    with pytest.raises(OverflowError, match=message):
        with lz4.stream.LZ4StreamCompressor(**kwargs) as proc:
            proc.compress(data)

    with pytest.raises(OverflowError, match=message):
        with lz4.stream.LZ4StreamDecompressor(**kwargs) as proc:
            proc.decompress(data)


@pytest.mark.skipif(
    os.environ.get('TRAVIS') is not None,
    reason='Skipping test on Travis due to insufficient memory'
)
@pytest.mark.skipif(
    sys.maxsize < 0xffffffff,
    reason='Py_ssize_t too small for this test'
)
@pytest.mark.skipif(
    psutil.virtual_memory().available < _4GB or huge is None,
    reason='Insufficient system memory for this test'
)
def test_huge_2():
    data = huge
    kwargs = {
        'strategy': "double_buffer",
        'buffer_size': lz4.stream.LZ4_MAX_INPUT_SIZE,
        'store_comp_size': 4,
        'dictionary': b'',
    }

    if psutil.virtual_memory().available < 3 * kwargs['buffer_size']:
        # The internal LZ4 context will request at least 3 times buffer_size
        # as memory (2 buffer_size for the double-buffer, and 1.x buffer_size
        # for the output buffer)
        pytest.skip('Insufficient system memory for this test')

    # Raising overflow error
    message = r'^Input too large for LZ4 API$'

    with pytest.raises(OverflowError, match=message):
        with lz4.stream.LZ4StreamCompressor(**kwargs) as proc:
            proc.compress(data)

    # On decompression, too large input will raise LZ4StreamError
    with pytest.raises(lz4.stream.LZ4StreamError):
        with lz4.stream.LZ4StreamDecompressor(**kwargs) as proc:
            proc.decompress(data)


@pytest.mark.skipif(
    os.environ.get('TRAVIS') is not None,
    reason='Skipping test on Travis due to insufficient memory'
)
@pytest.mark.skipif(
    sys.maxsize < 0xffffffff,
    reason='Py_ssize_t too small for this test'
)
@pytest.mark.skipif(
    psutil.virtual_memory().available < _4GB or huge is None,
    reason='Insufficient system memory for this test'
)
def test_huge_3():
    data = huge
    kwargs = {
        'strategy': "double_buffer",
        'buffer_size': lz4.stream.LZ4_MAX_INPUT_SIZE,
        'store_comp_size': 4,
        'dictionary': huge,
    }

    if psutil.virtual_memory().available < 3 * kwargs['buffer_size']:
        # The internal LZ4 context will request at least 3 times buffer_size
        # as memory (2 buffer_size for the double-buffer, and 1.x buffer_size
        # for the output buffer)
        pytest.skip('Insufficient system memory for this test')

    # Raising overflow error (during initialization because of the dictionary parameter)
    message = r'^Dictionary too large for LZ4 API$'

    with pytest.raises(OverflowError, match=message):
        with lz4.stream.LZ4StreamCompressor(**kwargs) as proc:
            proc.compress(data)

    with pytest.raises(OverflowError, match=message):
        with lz4.stream.LZ4StreamDecompressor(**kwargs) as proc:
            proc.decompress(data)


def test_dummy():
    pass
