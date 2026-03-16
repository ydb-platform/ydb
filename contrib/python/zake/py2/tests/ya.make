PY2TEST()

PEERDIR(
    contrib/python/zake
)

SRCDIR(contrib/python/zake/py2/zake/tests)

TEST_SRCS(
    test_client.py
)

NO_LINT()

END()
