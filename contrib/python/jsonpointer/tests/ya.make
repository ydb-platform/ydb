PY3TEST()

PEERDIR(
    contrib/python/jsonpointer
)

SRCDIR(contrib/python/jsonpointer)

TEST_SRCS(
    tests.py
)

NO_LINT()

END()
