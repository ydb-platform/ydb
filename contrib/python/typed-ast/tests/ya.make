PY3TEST()

NO_LINT()

PEERDIR(contrib/python/typed-ast)

SRCDIR(contrib/python/typed-ast)

TEST_SRCS(
    typed_ast/tests/test_basics.py
)

END()
