PY23_LIBRARY()

TEST_SRCS(
    conftest.py
    foo/conftest.py
    foo/bar/conftest.py
)

STYLE_PYTHON()

END()

RECURSE_FOR_TESTS(
    foo
)
