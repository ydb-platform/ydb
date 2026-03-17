PY3TEST()

PEERDIR(
    contrib/python/sobol-seq
)

TEST_SRCS(
    sobol_test.py
)

NO_LINT()

END()
