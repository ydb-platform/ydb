PY3TEST()
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
ENV(MOTO_SERVER_PATH="contrib/python/moto/bin/moto_server")
ENV(YDB_ADDITIONAL_LOG_CONFIGS="TX_TIERING:DEBUG")

# INCLUDE(${ARCADIA_ROOT}/ydb/tests/tools/fq_runner/ydb_runner_with_datastreams.inc)

REQUIREMENTS(ram:48 cpu:all)

TEST_SRCS(
    test_bulkupserts_tpch.py
    test_insertinto_selectfrom.py
    test_insert_delete_duplicate_records.py
    test_tiering.py
    test_workload_manager.py
)

SIZE(LARGE)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)

DEPENDS(
    ydb/apps/ydb
    ydb/apps/ydbd
    ydb/tests/sql/lib
    contrib/python/moto/bin
)

PEERDIR(
    ydb/tests/library
    ydb/tests/sql/lib
    contrib/python/moto
    contrib/python/boto3
)

END()
