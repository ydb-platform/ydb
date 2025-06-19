PY3TEST()
ENV(YDB_HARD_MEMORY_LIMIT_BYTES="107374182400")

TEST_SRCS(
    test_clean.py
    test_clickbench.py
    test_workload_simple_queue.py
    test_workload_oltp.py
    test_external.py
    test_diff_processing.py
    test_tpch.py
    test_upload.py
    test_import_csv.py
)

SIZE(MEDIUM)

REQUIREMENTS(ram:16)

ENV(YDB_ENABLE_COLUMN_TABLES="true")
INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)
ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")
ENV(NO_KUBER_LOGS="yes")
ENV(WAIT_CLUSTER_ALIVE_TIMEOUT="60")
ENV(ARCADIA_EXTERNAL_DATA=ydb/tests/functional/tpc/data)
ENV(SIMPLE_QUEUE_BINARY="ydb/tests/stress/simple_queue/simple_queue")
ENV(OLTP_WORKLOAD_BINARY="ydb/tests/stress/oltp_workload/oltp_workload")

PEERDIR(
    ydb/tests/functional/tpc/lib
)

DEPENDS(
    ydb/apps/ydb
    ydb/tests/stress/simple_queue
    ydb/tests/stress/oltp_workload
)

DATA(
    arcadia/ydb/tests/functional/clickbench/data/hits.csv
    arcadia/ydb/tests/functional/tpc/data
)

FORK_TEST_FILES()
REQUIREMENTS(ram:28)
END()
