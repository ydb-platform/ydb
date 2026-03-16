PY3TEST()

SIZE(MEDIUM)

FORK_SUBTESTS()

PEERDIR(
    contrib/python/prophet
)

DATA(
    arcadia/contrib/python/prophet/prophet/tests
)

NO_LINT()

SRCDIR(contrib/python/prophet/prophet/tests)

TEST_SRCS(
    __init__.py
    test_diagnostics.py
    test_prophet.py
    test_serialize.py
    test_utilities.py
)

END()
