PY3TEST()

SIZE(LARGE)

TAG(
    ya:fat
)

PY_SRCS(
    run_tests/run_tests.py
)

TEST_SRCS(
    tpc_tests.py
)

END()

RECURSE(
    run_tests
    runner
    result_convert
    result_compare
)
