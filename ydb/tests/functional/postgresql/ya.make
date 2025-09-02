PY3TEST()

DATA(
    arcadia/ydb/tests/functional/postgresql/cases
)

DEPENDS(
    ydb/tests/functional/postgresql/psql
)


ENV(PYTHONWARNINGS="ignore")
INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)
ENV(YDB_TABLE_ENABLE_PREPARED_DDL=true)
ENV(YDB_USE_IN_MEMORY_PDISKS=true)
ENV(YDB_ALLOCATE_PGWIRE_PORT=true)
ENV(YDB_ALLOCATE_PGWIRE_PORT=true)

SIZE(MEDIUM)

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
