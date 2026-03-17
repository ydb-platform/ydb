PY3TEST()

PEERDIR(
    contrib/python/line-profiler
    contrib/python/ipython
)

NO_LINT()

TEST_SRCS(
    test_line_profiler.py
    test_kernprof.py
    test_ipython.py
)

END()
