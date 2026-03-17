PY3TEST()

PEERDIR(
    contrib/python/wtf-peewee
)

SRCDIR(contrib/python/wtf-peewee/wtfpeewee)

TEST_SRCS(
    tests.py
)

NO_LINT()

END()
