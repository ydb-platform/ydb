PY23_LIBRARY()

TEST_SRCS(
    foo/conftest.py
    foo_bar/conftest.py
)

STYLE_PYTHON()

END()

RECURSE_FOR_TESTS(
    foo
    foo_bar
)
