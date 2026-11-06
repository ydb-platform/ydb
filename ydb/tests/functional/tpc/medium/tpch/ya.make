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
IF (NOT SANITIZER_TYPE)
    REQUIREMENTS(cpu:3)
ELSE()
    REQUIREMENTS(cpu:2)
ENDIF()
END()

RECURSE_FOR_TESTS(
    duplicates
    parallel
    s1
)
