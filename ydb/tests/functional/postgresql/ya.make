PY3TEST()

DATA(
    arcadia/ydb/tests/functional/postgresql/cases
    sbr://4298147653=psql
)

DEPENDS(
    ydb/apps/ydbd
    ydb/apps/pgwire
)

ENV(PYTHONWARNINGS="ignore")
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
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
