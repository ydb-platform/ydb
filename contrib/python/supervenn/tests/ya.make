PY3TEST()

PEERDIR(
    contrib/python/supervenn
    contrib/python/numpy
    contrib/python/matplotlib
)

TEST_SRCS(
    test_plots.py
)

NO_LINT()

END()

