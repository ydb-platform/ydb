PY3TEST()

TEST_SRCS(
    test_ydb_bench.py
    test_ydb_telemetry.py
    test_hosts.py
    test_cluster_templates.py
)

PEERDIR(
    ydb/tools/ydb_bench/benchmarks
    ydb/tools/ydb_bench/lib
)

END()
