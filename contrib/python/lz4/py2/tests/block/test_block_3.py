import lz4.block
import pytest


test_data = [
    (b'a' * 1024 * 1024),
]


@pytest.fixture(
    params=test_data,
    ids=[
        'data' + str(i) for i in range(len(test_data))
    ]
)
def data(request):
    return request.param


def test_block_decompress_mem_usage(data):
    tracemalloc = pytest.importorskip('tracemalloc')

    tracemalloc.start()

    compressed = lz4.block.compress(data)
    prev_snapshot = None

    for i in range(1000):
        decompressed = lz4.block.decompress(compressed)  # noqa: F841

        if i % 100 == 0:
            snapshot = tracemalloc.take_snapshot()

            if prev_snapshot:
                stats = snapshot.compare_to(prev_snapshot, 'lineno')
                assert stats[0].size_diff < (1024 * 4)

            prev_snapshot = snapshot
