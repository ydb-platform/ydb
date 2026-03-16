PY2TEST()

NO_LINT()

TEST_SRCS(
    conftest.py
    test_async.py
    test_fixtures.py
    test_param.py
    test_server.py
)

PEERDIR(
    contrib/python/pytest-tornado
)

END()
