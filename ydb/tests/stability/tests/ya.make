PY3TEST()

    TAG(ya:manual)

    SIZE(LARGE)

    ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")
    ENV(NEMESIS_BINARY="ydb/tests/tools/nemesis/driver/nemesis")

    TEST_SRCS (
        test_workload_parallel.py
        test_per_workload.py
        all_workloads.py
    )

    PEERDIR (
        ydb/tests/library/stability
    )

    IF(NOT NOT_INCLUDE_CLI)
        DEPENDS (
            ydb/apps/ydb
            ydb/tests/stress/simple_queue
            ydb/tests/stress/topic
            ydb/tests/stress/log
            ydb/tests/stress/mixedpy
            ydb/tests/stress/kv
            ydb/tests/stress/oltp_workload
            ydb/tests/stress/olap_workload
            ydb/tests/stress/ctas
            ydb/tests/stress/kafka
            ydb/tests/stress/node_broker
            ydb/tests/stress/topic_kafka
            ydb/tests/stress/transfer
            ydb/tests/stress/reconfig_state_storage_workload
            ydb/tests/stress/show_create/view
            ydb/tests/stress/cdc
            ydb/tests/stress/statistics_workload
            ydb/tests/stress/viewer
            ydb/tests/tools/nemesis/driver
        )
    ENDIF()

END()
