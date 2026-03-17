PY3TEST()

NO_LINT()

PEERDIR(
    contrib/python/lxml
    contrib/python/htmldate
)

TEST_SRCS(
    unit_tests.py
)

DATA(
    arcadia/contrib/python/htmldate/tests
)

END()
