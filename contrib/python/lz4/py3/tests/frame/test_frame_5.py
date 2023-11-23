import lz4.frame
import pytest
import gc

MEM_INCREASE_LIMIT = (1024 * 25)

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


def test_frame_decompress_mem_usage(data):
    tracemalloc = pytest.importorskip('tracemalloc')

    tracemalloc.start()

    compressed = lz4.frame.compress(data)
    prev_snapshot = None

    for i in range(1000):
        decompressed = lz4.frame.decompress(compressed)  # noqa: F841

        if i % 100 == 0:
            gc.collect()
            snapshot = tracemalloc.take_snapshot()

            if prev_snapshot:
                stats = snapshot.compare_to(prev_snapshot, 'lineno')
                assert stats[0].size_diff < MEM_INCREASE_LIMIT

            prev_snapshot = snapshot


def test_frame_decompress_chunk_mem_usage(data):
    tracemalloc = pytest.importorskip('tracemalloc')
    tracemalloc.start()

    compressed = lz4.frame.compress(data)

    prev_snapshot = None

    for i in range(1000):
        context = lz4.frame.create_decompression_context()
        decompressed = lz4.frame.decompress_chunk(  # noqa: F841
            context, compressed
        )

        if i % 100 == 0:
            gc.collect()
            snapshot = tracemalloc.take_snapshot()

            if prev_snapshot:
                stats = snapshot.compare_to(prev_snapshot, 'lineno')
                assert stats[0].size_diff < MEM_INCREASE_LIMIT

            prev_snapshot = snapshot


def test_frame_open_decompress_mem_usage(data):
    tracemalloc = pytest.importorskip('tracemalloc')
    tracemalloc.start()

    with lz4.frame.open('test.lz4', 'w') as f:
        f.write(data)

    prev_snapshot = None

    for i in range(1000):
        with lz4.frame.open('test.lz4', 'r') as f:
            decompressed = f.read()  # noqa: F841

        if i % 100 == 0:
            gc.collect()
            snapshot = tracemalloc.take_snapshot()

            if prev_snapshot:
                stats = snapshot.compare_to(prev_snapshot, 'lineno')
                assert stats[0].size_diff < MEM_INCREASE_LIMIT

            prev_snapshot = snapshot


# TODO: add many more memory usage tests along the lines of this one
# for other funcs

def test_dummy_always_pass():
    # If pytest finds all tests are skipped, then it exits with code 5 rather
    # than 0, which tox sees as an error. Here we add a dummy test that always passes.
    assert True
