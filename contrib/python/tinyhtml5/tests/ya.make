PY3TEST()

FORK_SUBTESTS()

PEERDIR(
    contrib/python/tinyhtml5
)

DATA(
    arcadia/contrib/python/tinyhtml5/tests
)

ALL_PYTEST_SRCS()

NO_LINT()

END()
