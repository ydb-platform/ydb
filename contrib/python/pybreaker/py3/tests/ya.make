PY3TEST()

PEERDIR(
    contrib/python/pybreaker
    contrib/python/fakeredis
    contrib/python/tornado
)

SRCDIR(
    contrib/python/pybreaker/py3
)

TEST_SRCS(
    pybreaker_test.py
)

NO_LINT()

END()
