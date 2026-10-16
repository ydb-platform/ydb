PY3TEST()

TEST_SRCS(test.py)

REQUIREMENTS(cpu:1)
PEERDIR(
    ydb/library/benchmarks/report
)

END()
