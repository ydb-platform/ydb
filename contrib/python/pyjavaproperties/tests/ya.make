PY3TEST()

PEERDIR(
    contrib/python/pyjavaproperties
)

SRCDIR(contrib/python/pyjavaproperties)

TEST_SRCS(
    pyjavaproperties_test.py
)

DATA(
    arcadia/contrib/python/pyjavaproperties/tests/testdata
)

NO_LINT()

END()
