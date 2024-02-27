PY3TEST()

OWNER(g:yql)

TEST_SRCS(test.py)

RESOURCE(test.txt test.txt)

PEERDIR(
    ydb/library/benchmarks/template
)

END()
