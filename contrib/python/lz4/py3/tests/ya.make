PY3TEST()

PEERDIR(
    contrib/python/lz4
    contrib/python/psutil
)

FORK_SUBTESTS()
SIZE(MEDIUM)

TEST_SRCS(
    block/conftest.py
    #block/test_block_0.py
    block/test_block_1.py
    block/test_block_2.py
    block/test_block_3.py
    frame/__init__.py
    frame/conftest.py
    frame/helpers.py
    frame/test_frame_0.py
    frame/test_frame_1.py
    frame/test_frame_2.py
    frame/test_frame_3.py
    frame/test_frame_4.py
    frame/test_frame_5.py
    frame/test_frame_6.py
    frame/test_frame_7.py
    frame/test_frame_8.py
    frame/test_frame_9.py
    stream/conftest.py
    stream/test_stream_0.py
    #stream/test_stream_1.py
    stream/test_stream_2.py
    stream/test_stream_3.py
    stream/test_stream_4.py
)

NO_LINT()

REQUIREMENTS(ram:18)

END()
