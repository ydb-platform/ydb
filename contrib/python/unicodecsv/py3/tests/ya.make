PY3TEST()

PEERDIR(
    contrib/python/unicodecsv
)

SRCDIR(contrib/python/unicodecsv/py3/unicodecsv)

TEST_SRCS(
    test.py
)

NO_LINT()

END()
