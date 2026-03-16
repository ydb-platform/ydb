PY3TEST()

PEERDIR(
    contrib/python/typed-argument-parser
)

NO_LINT()

FORK_TESTS()

ALL_PYTEST_SRCS(RECURSIVE)

END()
