import pytest
import sys
import lz4.block
import psutil
import os

# This test requires allocating a big lump of memory. In order to
# avoid a massive memory allocation during byte compilation, we have
# to declare a variable for the size of the buffer we're going to
# create outside the scope of the function below. See:
# https://bugs.python.org/issue21074
_4GB = 0x100000000  # 4GB

# This test will be killed on Travis due to the 3GB memory limit
# there. Unfortunately psutil reports the host memory, not the memory
# available to the container, and so can't be used to detect available
# memory, so instead, as an ugly hack for detecting we're on Travis we
# check for the TRAVIS environment variable being set. This is quite
# fragile.


@pytest.mark.skipif(
    os.environ.get('TRAVIS') is not None,
    reason='Skipping test on Travis due to insufficient memory'
)
@pytest.mark.skipif(
    sys.maxsize < 0xffffffff,
    reason='Py_ssize_t too small for this test'
)
@pytest.mark.skipif(
    psutil.virtual_memory().available < _4GB,
    reason='Insufficient system memory for this test'
)
def test_huge():
    try:
        huge = b'\0' * _4GB
    except MemoryError:
        pytest.skip('Insufficient system memory for this test')

    with pytest.raises(
            OverflowError, match='Input too large for LZ4 API'
    ):
        lz4.block.compress(huge)

    with pytest.raises(
            OverflowError, match='Dictionary too large for LZ4 API'
    ):
        lz4.block.compress(b'', dict=huge)

    with pytest.raises(
            OverflowError, match='Input too large for LZ4 API'
    ):
        lz4.block.decompress(huge)

    with pytest.raises(
            OverflowError, match='Dictionary too large for LZ4 API'
    ):
        lz4.block.decompress(b'', dict=huge)


def test_dummy():
    pass
