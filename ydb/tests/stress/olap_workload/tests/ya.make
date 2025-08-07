PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)
ENV(YDB_ENABLE_COLUMN_TABLES="true")
ENV(YDB_WORKLOAD_PATH="ydb/tests/stress/olap_workload/olap_workload")

TEST_SRCS(
    test_workload.py
)

REQUIREMENTS(ram:32)

SIZE(MEDIUM)

DEPENDS(
    ydb/tests/stress/olap_workload
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/stress
    ydb/tests/stress/common
)

END()
