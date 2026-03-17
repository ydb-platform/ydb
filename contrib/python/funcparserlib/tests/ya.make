PY3TEST()

PEERDIR(
    contrib/python/funcparserlib
)

TEST_SRCS(
    __init__.py
    dot.py
    json.py
    test_dot.py
    test_json.py
    test_parsing.py
)

NO_LINT()

END()
