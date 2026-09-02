PY3TEST()

TEST_SRCS(
    test_ydb_bench.py
)

PEERDIR(
    ydb/tools/ydb_bench/benchmarks
    ydb/tools/ydb_bench/lib
)

END()
