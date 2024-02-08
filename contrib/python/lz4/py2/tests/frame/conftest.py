import pytest
import lz4.frame as lz4frame
import lz4


@pytest.fixture(
    params=[
        #        (lz4frame.BLOCKSIZE_DEFAULT),
        (lz4frame.BLOCKSIZE_MAX64KB),
        (lz4frame.BLOCKSIZE_MAX256KB),
        (lz4frame.BLOCKSIZE_MAX1MB),
        (lz4frame.BLOCKSIZE_MAX4MB),
    ]
)
def block_size(request):
    return request.param


@pytest.fixture(
    params=[
        (True),
        (False),
    ]
)
def block_linked(request):
    return request.param


@pytest.fixture(
    params=[
        (True),
        (False),
    ]
)
def content_checksum(request):
    return request.param


if lz4.library_version_number() >= 10800:
    p = [True, False]
else:
    p = [False, ]


@pytest.fixture(
    params=[
        (pp) for pp in p
    ]
)
def block_checksum(request):
    return request.param


compression_levels = [
    (lz4frame.COMPRESSIONLEVEL_MIN),
    (lz4frame.COMPRESSIONLEVEL_MINHC),
    (lz4frame.COMPRESSIONLEVEL_MAX),
]


@pytest.fixture(
    params=compression_levels
)
def compression_level(request):
    return request.param


@pytest.fixture(
    params=[
        (True),
        (False)
    ]
)
def auto_flush(request):
    return request.param


@pytest.fixture(
    params=[
        (True),
        (False)
    ]
)
def store_size(request):
    return request.param


@pytest.fixture(
    params=[
        (True),
        (False),
    ]
)
def return_bytearray(request):
    return request.param
