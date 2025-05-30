PY3TEST()

    TAG(ya:manual)

    SIZE(LARGE)

    ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")
    ENV(SIMPLE_QUEUE_BINARY="ydb/tests/stress/simple_queue/simple_queue")

    TEST_SRCS (
        test_clickbench.py
        test_external.py
        test_tpcds.py
        test_tpch.py
        test_workload_simple_queue.py
    )

    PEERDIR (
        ydb/tests/olap/load/lib
    )

    IF(NOT NOT_INCLUDE_CLI)
        DEPENDS (
            ydb/apps/ydb
            ydb/tests/stress/simple_queue
        )
    ENDIF()

END()
