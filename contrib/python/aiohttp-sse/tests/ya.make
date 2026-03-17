PY3TEST()

PEERDIR(
    contrib/python/aiohttp
    contrib/python/aiohttp-sse
    contrib/python/pytest-aiohttp
)

NO_LINT()

TEST_SRCS(
    test_sse.py
)

END()
