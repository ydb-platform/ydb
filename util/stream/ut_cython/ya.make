PY23_TEST()

SRCDIR(util/stream)

PY_SRCS(
    NAMESPACE util.stream
    str_ut.pyx
)

TEST_SRCS(
    test_stream.py
)

END()
