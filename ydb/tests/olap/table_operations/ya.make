PY3TEST()

SRCS(
    GLOBAL
    test_table_rename.py
)

PEERDIR(
    ydb/tests/olap/lib
    ydb/public/sdk/python3
    library/python/testing/hamcrest
)

DATA(
    arcadia/ydb/tests/olap/config
)
