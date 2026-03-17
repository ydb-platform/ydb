PY3TEST()

PEERDIR(
    contrib/python/aresponses
)

SRCDIR(contrib/python/aresponses)

PY_SRCS(
    __init__.py
    conftest.py
)

TEST_SRCS(
    test_examples.py
    test_server.py
    test_with_aiohttp_client.py
)

END()
