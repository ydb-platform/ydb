PY3TEST()

SIZE(MEDIUM)

TEST_SRCS(
    test_furl.py
    test_omdict1D.py
)

PEERDIR(
    contrib/python/furl
)

NO_LINT()

END()
