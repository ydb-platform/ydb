PY3TEST()

PEERDIR(
    contrib/python/janus
    contrib/python/pytest-asyncio
)

TEST_SRCS(
    test_async.py
    test_mixed.py
    test_sync.py
)

END()
