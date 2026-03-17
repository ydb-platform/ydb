PY3TEST()

PEERDIR(
    contrib/python/importlib-metadata
    contrib/python/pycountry
)

SRCDIR(contrib/python/pycountry/pycountry/tests)

TEST_SRCS(
    test_general.py
)

NO_LINT()

END()
