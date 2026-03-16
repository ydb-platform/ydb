PY3TEST()

PEERDIR(
    contrib/python/logging-tree
)

SRCDIR(contrib/python/logging-tree/logging_tree/tests)

PY_SRCS(
    NAMESPACE logging_tree.tests
    __init__.py
    case.py
)

TEST_SRCS(
    test_format.py
    test_node.py
)

NO_LINT()

END()
