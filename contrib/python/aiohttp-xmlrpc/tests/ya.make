PY3TEST()

PEERDIR(
    contrib/python/aiohttp-xmlrpc
    contrib/python/xmltodict
)

TEST_SRCS(
    __init__.py
    conftest.py
    test_common.py
    test_handler.py
)

NO_LINT()

END()
