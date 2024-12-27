PY3TEST()
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")

REQUIREMENTS(ram:48)

TEST_SRCS(
    test_bulkupserts_tpch.py
    test_insertinto_selectfrom.py
    test_insert_delete_duplicate_records.py
)

SIZE(LARGE)
TAG(ya:fat)

DEPENDS(
    ydb/apps/ydb
    ydb/apps/ydbd
    ydb/tests/sql/lib
)

PEERDIR(
    ydb/tests/library
    ydb/tests/sql/lib
)

FORK_SUBTESTS()

END()
