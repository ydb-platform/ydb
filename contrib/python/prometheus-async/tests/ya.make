PY3TEST()

PEERDIR(
    contrib/python/aiohttp
    contrib/python/pytest
    contrib/python/pytest-asyncio
    contrib/python/wrapt
    contrib/python/prometheus-client
    contrib/python/prometheus-async
)

TEST_SRCS(
    conftest.py
    test_aio.py
)

END()
