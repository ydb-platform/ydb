PY2TEST()

PEERDIR(
    contrib/python/unicodecsv
)

SRCDIR(contrib/python/unicodecsv/py2/unicodecsv)

TEST_SRCS(
    test.py
)

NO_LINT()

END()
