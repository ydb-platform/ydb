PY3TEST()

PEERDIR(
    contrib/python/jsonobject
)

TEST_SRCS(
    test_stringconversions.py
    tests.py
)

NO_LINT()

END()