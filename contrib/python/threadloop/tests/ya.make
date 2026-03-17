PY3TEST()

PEERDIR(
    contrib/python/tornado/tornado-4
    contrib/python/threadloop
)

TEST_SRCS(
    threadloop/__init__.py
    threadloop/test_threadloop.py
)

END()
