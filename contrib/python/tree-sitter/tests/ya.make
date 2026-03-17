PY3TEST()

NO_COMPILER_WARNINGS()

NO_LINT()

PEERDIR(
    contrib/libs/tree_sitter/core
    contrib/python/tree-sitter
)

TEST_SRCS(test_tree_sitter.py)

END()
