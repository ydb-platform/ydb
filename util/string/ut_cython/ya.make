PY23_TEST()

SRCDIR(util/string)

PY_SRCS(
    NAMESPACE util.string
    cast_ut.pyx
)

TEST_SRCS(
    test_string.py
)

END()
