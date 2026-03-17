PY2TEST()

PEERDIR(
    contrib/python/inflection
)

SRCDIR(contrib/python/inflection/py2)

TEST_SRCS(
    test_inflection.py
)

NO_LINT()

END()
