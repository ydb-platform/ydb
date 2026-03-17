PY3TEST()

PEERDIR(
    contrib/python/webencodings
)

SRCDIR(contrib/python/webencodings/py3/webencodings)

TEST_SRCS(
    tests.py
)

NO_LINT()

END()
