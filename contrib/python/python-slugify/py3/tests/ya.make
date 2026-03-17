PY3TEST()

PEERDIR(
    contrib/python/python-slugify
)

SRCDIR(
    contrib/python/python-slugify/py3
)

NO_LINT()

TEST_SRCS(
    test.py
)

END()
