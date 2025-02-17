PY3TEST()

    TAG(ya:manual)

    SIZE(LARGE)

    ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")

    TEST_SRCS (
        test_clickbench.py
        test_tpcds.py
        test_tpch.py
    )

    PEERDIR (
        ydb/tests/olap/load/lib
    )

    IF(NOT NOT_INCLUDE_CLI)
        DEPENDS (
            ydb/apps/ydb
        )
    ENDIF()

END()
