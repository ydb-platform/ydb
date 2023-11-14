PY2TEST()

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
)

NO_LINT()

END()
