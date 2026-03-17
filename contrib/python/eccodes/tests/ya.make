PY3TEST()

PEERDIR(
    contrib/python/eccodes
)

TEST_SRCS(
    test_20_main.py
    test_20_messages.py
    test_eccodes.py
    test_highlevel.py
)

DATA(
    arcadia/contrib/python/eccodes/tests/sample-data
)

TEST_CWD(/)

NO_LINT()

END()
