PY3TEST()

PEERDIR(
    contrib/python/commonmark
)

SRCDIR(
    contrib/python/commonmark/commonmark/tests
)

TEST_SRCS(
    __init__.py
    rst_tests.py
    run_spec_tests.py
    unit_tests.py
)

NO_LINT()

END()
