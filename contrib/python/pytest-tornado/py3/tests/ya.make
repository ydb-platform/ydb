PY3TEST()

NO_LINT()

TEST_SRCS(
    conftest.py
    test_async.py
    test_async_await.py
    test_fixtures.py
    test_param.py
    test_server.py
)

PEERDIR(
    contrib/python/pytest-tornado
    contrib/python/tornado/tornado-6
)

END()
