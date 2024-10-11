PY2TEST()

SUBSCRIBER(g:python-contrib)

PEERDIR(
    contrib/python/pytest-mock/py2
)

TEST_SRCS(
    test_pytest_mock.py
)

NO_LINT()

END()
