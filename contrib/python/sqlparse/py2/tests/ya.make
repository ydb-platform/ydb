PY2TEST()

PEERDIR(
    contrib/python/sqlparse
)

TEST_SRCS(
    __init__.py
    conftest.py
    test_format.py
    test_grouping.py
    test_keywords.py
    test_parse.py
    test_regressions.py
    test_split.py
    test_tokenize.py
)

DATA(
    arcadia/contrib/python/sqlparse/py2/tests/files
)

NO_LINT()

END()
