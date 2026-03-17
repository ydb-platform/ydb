PY2TEST()

PEERDIR(
    contrib/python/flatbuffers/py2/tests/pregen_lib
)

TEST_SRCS(
    test.py
)

NO_LINT()

END()
