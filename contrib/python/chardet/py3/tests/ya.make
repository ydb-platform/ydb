PY3TEST()

SRCDIR(contrib/python/chardet/py3)

TEST_SRCS(
    test.py
)

PEERDIR(
    contrib/python/chardet
)

DATA(
    sbr://3544728585
)

SIZE(MEDIUM)

NO_LINT()

END()
