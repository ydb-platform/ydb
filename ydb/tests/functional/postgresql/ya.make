IF (NOT SANITIZER_TYPE AND NOT WITH_VALGRIND)

PY3TEST()

DATA(
    arcadia/ydb/tests/functional/postgresql/cases
)

DEPENDS(
    ydb/apps/ydbd
    ydb/tests/functional/postgresql/psql
)


ENV(PYTHONWARNINGS="ignore")
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
ENV(YDB_TABLE_ENABLE_PREPARED_DDL=true)
ENV(YDB_USE_IN_MEMORY_PDISKS=true)

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

TEST_SRCS(
    test_postgres.py
)

PEERDIR(
    library/python/testing/yatest_common
    ydb/tests/library
    ydb/tests/oss/ydb_sdk_import
    ydb/tests/functional/postgresql/common
)

END()

ENDIF()
