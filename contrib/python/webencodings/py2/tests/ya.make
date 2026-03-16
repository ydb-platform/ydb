PY2TEST()

PEERDIR(
    contrib/python/webencodings
)

SRCDIR(contrib/python/webencodings/py2/webencodings)

TEST_SRCS(
    tests.py
)

NO_LINT()

END()
