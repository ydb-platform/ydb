PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)
ENV(YDB_ENABLE_COLUMN_TABLES="true")

TEST_SRCS(
    test_kv.py
    test_crud.py
    test_inserts.py
)

SIZE(MEDIUM)

DEPENDS(
    ydb/apps/ydb
    ydb/tests/sql/lib
)

PEERDIR(
    ydb/tests/library
    ydb/tests/sql/lib
)

END()

RECURSE(
    lib
    large
)
