PY3TEST()

TEST_SRCS(
    test_decimal.py
    test_s_float.py
)

PY_SRCS(
    TOP_LEVEL
    s1/test_s1.py
)

INCLUDE(common.inc)

FORK_TEST_FILES()
FORK_SUBTESTS()
END()

RECURSE_FOR_TESTS(
    duplicates
    parallel
    s1
)
