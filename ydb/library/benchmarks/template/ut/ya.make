PY3TEST()

TEST_SRCS(test.py)

RESOURCE(test.txt test.txt)

REQUIREMENTS(cpu:1)
PEERDIR(
    ydb/library/benchmarks/template
)

END()
