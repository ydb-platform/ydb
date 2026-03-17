PY3TEST()

PEERDIR(
    contrib/python/catalogue
)

SRCDIR(contrib/python/catalogue/catalogue/tests)

TEST_SRCS(
    __init__.py
    test_catalogue.py
)

NO_LINT()

END()
