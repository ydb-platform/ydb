PY3TEST()

PEERDIR(
    contrib/python/py-expression-eval
)

SRCDIR(
    contrib/python/py-expression-eval/py_expression_eval
)

TEST_SRCS(
    tests.py
)

NO_LINT()

END()

