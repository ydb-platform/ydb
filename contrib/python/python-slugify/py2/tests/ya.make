PY2TEST()

PEERDIR(
    contrib/python/python-slugify
)

SRCDIR(
    contrib/python/python-slugify/py2
)

NO_LINT()

TEST_SRCS(
    test.py
)

END()
