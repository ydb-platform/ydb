PY3TEST()

TEST_SRCS(
    mimeparse_test.py
)

PEERDIR(
    contrib/python/python-mimeparse
)

DATA(
    arcadia/contrib/python/python-mimeparse/py3/tests/testdata.json
)

NO_LINT()

END()
