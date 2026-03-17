PY3TEST()

PEERDIR(
    contrib/python/flatbuffers/py3/tests/pregen_lib
)

TEST_SRCS(
    test.py
)

NO_LINT()

END()
