PY3TEST()

PEERDIR(
    contrib/python/scales
)

SRCDIR(contrib/python/scales/greplin/scales)

TEST_SRCS(
    aggregation_test.py
    formats_test.py
    samplestats_test.py
    scales_test.py
    util_test.py
)

NO_LINT()

END()
