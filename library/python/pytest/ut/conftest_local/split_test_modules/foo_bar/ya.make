PY23_TEST()

SIZE(SMALL)

ALL_PYTEST_SRCS(ONLY_TEST_FILES)

PEERDIR(
    library/python/pytest
    library/python/pytest/ut/conftest_local/split_test_modules
)

CONFTEST_LOAD_POLICY_LOCAL()

STYLE_PYTHON()

END()
