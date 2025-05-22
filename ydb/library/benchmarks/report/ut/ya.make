PY3TEST()

SUBSCRIBER(g:yql)

TEST_SRCS(test.py)

PEERDIR(
    ydb/library/benchmarks/report
)

END()
