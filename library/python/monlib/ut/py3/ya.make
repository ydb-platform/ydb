PY3TEST()

TEST_SRCS(
    test_metric.py
    test.py
)

SRCDIR(
    library/python/monlib/ut
)

PEERDIR(
    library/python/monlib
    library/python/monlib/ut
)

END()
