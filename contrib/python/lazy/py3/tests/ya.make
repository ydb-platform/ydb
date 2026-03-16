PY3TEST()

PEERDIR(
    contrib/python/lazy
)

SRCDIR(
    contrib/python/lazy/py3/lazy/tests
)

TEST_SRCS(
    test_lazy.py
)

NO_LINT()

END()
