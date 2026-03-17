PY2TEST()

PEERDIR(
    contrib/python/uritemplate
)

TEST_SRCS(
    test_from_fixtures.py
    test_uritemplate.py
)

DATA(
    arcadia/contrib/python/uritemplate/py2/tests
)

NO_LINT()

END()
