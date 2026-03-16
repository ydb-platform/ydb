PY3TEST()

PEERDIR(
    contrib/python/docstring-parser
)

SRCDIR(contrib/python/docstring-parser/docstring_parser/tests)

TEST_SRCS(
    __init__.py
    test_google.py
    test_epydoc.py
    test_numpydoc.py
    test_parser.py
    test_rest.py
    test_util.py
)

NO_LINT()

END()
