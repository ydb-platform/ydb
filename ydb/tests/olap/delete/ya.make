PY3TEST()
    ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")

    FORK_SUBTESTS()

    TEST_SRCS(
        base.py
        test_delete_by_explicit_row_id.py
    )

    SIZE(MEDIUM)

    PEERDIR(
        ydb/tests/library
        ydb/tests/library/test_meta
        ydb/tests/olap/common
    )

    DEPENDS(
        ydb/apps/ydbd
    )

END()
