PY23_TEST()

PY_SRCS(
    conftest.py
)

ALL_PYTEST_SRCS(ONLY_TEST_FILES)

PEERDIR(
    library/python/pytest
    library/python/pytest/ut/conftest_non_local
    library/python/pytest/ut/conftest_non_local/lib
    library/python/pytest/ut/conftest_non_local/lib2
    library/python/pytest/ut/conftest_non_local/tests/lib_child
)

STYLE_PYTHON()

END()
