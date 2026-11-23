PY3TEST()

TEST_SRCS(test_channel.py)

PEERDIR(
    contrib/python/grpcio
    contrib/python/pytest-asyncio
)

END()
