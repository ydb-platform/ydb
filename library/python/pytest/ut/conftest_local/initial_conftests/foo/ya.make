PY23_TEST()

TEST_SRCS(
    bar/test_initial_conftests.py
)

PEERDIR(
    library/python/pytest
    library/python/pytest/ut/conftest_local/initial_conftests
)

CONFTEST_LOAD_POLICY_LOCAL()

STYLE_PYTHON()

END()
