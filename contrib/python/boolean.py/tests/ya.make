PY3TEST()

PEERDIR(
    contrib/python/boolean.py
)

SRCDIR(contrib/python/boolean.py/boolean)

TEST_SRCS(
    test_boolean.py
)

NO_LINT()

END()
