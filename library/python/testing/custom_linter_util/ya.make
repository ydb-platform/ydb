PY3_LIBRARY()

PY_SRCS(
    linter_params.py
    reporter.py
)

END()

RECURSE_FOR_TESTS(
    tests
)
