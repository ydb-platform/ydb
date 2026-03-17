PY3TEST()

PEERDIR(
    contrib/python/aiohttp-remotes
    contrib/python/pytest-aiohttp
)

TEST_SRCS(
    conftest.py
    test_allowed_hosts.py
    test_basic_auth.py
    test_cloudfare.py
    test_forwarded.py
    test_secure.py
    test_utils.py
    test_x_forwarded.py
)

END()
