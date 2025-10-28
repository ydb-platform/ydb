PY3TEST()
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)
    ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")

    FORK_SUBTESTS()

    TEST_SRCS(
        base.py
        test_select_chunked_data.py
    )

    SIZE(MEDIUM)

    PEERDIR(
        ydb/tests/library
        ydb/tests/library/test_meta
        ydb/tests/olap/common
    )

    DEPENDS(
        ydb/apps/ydb
    )
END()
