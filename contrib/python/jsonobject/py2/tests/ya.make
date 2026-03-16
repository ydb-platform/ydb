PY2TEST()

PEERDIR(
    contrib/python/jsonobject
    contrib/python/six
)

TEST_SRCS(
    test_stringconversions.py
    tests.py
)

NO_LINT()

END()