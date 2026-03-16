PY3TEST()

NO_LINT()

SRCDIR(contrib/python/pykdtree/pykdtree)

TEST_SRCS(
    test_tree.py
)

PEERDIR(
    contrib/python/pykdtree
)

END()
