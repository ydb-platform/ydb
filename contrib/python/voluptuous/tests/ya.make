PY3TEST()

PEERDIR(
    contrib/python/voluptuous
)

DATA(
    arcadia/contrib/python/voluptuous/voluptuous/tests
)

SRCDIR(contrib/python/voluptuous/voluptuous/tests)

TEST_SRCS(
    tests.py
)

NO_LINT()

END()
