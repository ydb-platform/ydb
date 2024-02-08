PY2TEST()

SRCDIR(contrib/python/chardet/py2)

TEST_SRCS(
    test.py
)

PEERDIR(
    contrib/python/chardet
)

DATA(
    sbr://405525759
)

SIZE(MEDIUM)

NO_LINT()

END()
