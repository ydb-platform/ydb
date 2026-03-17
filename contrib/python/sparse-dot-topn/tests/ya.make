PY3TEST()

SIZE(MEDIUM)

FORK_TESTS()

NO_LINT()

TEST_SRCS(
    sparse_dot_topn/test_awesome_cossim_topn.py
)

PEERDIR(
    contrib/python/numpy
    contrib/python/pandas
    contrib/python/scipy
    contrib/python/sparse-dot-topn
)

END()
