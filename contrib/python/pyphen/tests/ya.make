PY3TEST()

FORK_SUBTESTS()

PEERDIR(
    contrib/python/pyphen
)

NO_LINT()

DATA(
    arcadia/contrib/python/pyphen
)

TEST_SRCS(
    __init__.py
    test_pyphen.py
)

END()
