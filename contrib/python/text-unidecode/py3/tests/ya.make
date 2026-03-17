PY3TEST()

WITHOUT_LICENSE_TEXTS()

PEERDIR(
    contrib/python/text-unidecode
)

TEST_SRCS(
    test_unidecode.py
)

NO_LINT()

END()
