PY3TEST()

    TAG(ya:manual)

    SIZE(LARGE)

    ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")
    ENV(SIMPLE_QUEUE_BINARY="ydb/tests/stress/simple_queue/simple_queue")
    ENV(OLTP_WORKLOAD_BINARY="ydb/tests/stress/oltp_workload/oltp_workload")
    ENV(OLAP_WORKLOAD_BINARY="ydb/tests/stress/olap_workload/olap_workload")
    ENV(TOPIC_WORKLOAD_BINARY="ydb/tests/stress/topic/workload_topic")
    ENV(LOG_WORKLOAD_BINARY="ydb/tests/stress/log/workload_log")
    ENV(MIXED_WORKLOAD_BINARY="ydb/tests/stress/mixedpy/workload_mixed")
    ENV(KV_WORKLOAD_BINARY="ydb/tests/stress/kv/workload_kv")
    ENV(NEMESIS_BINARY="ydb/tests/tools/nemesis/driver/nemesis")

    TEST_SRCS (
        test_clickbench.py
        test_external.py
        test_import_csv.py
        test_tpcds.py
        test_tpch.py
        test_upload.py
        test_workload_manager.py
        test_workload_simple_queue.py
        test_workload_oltp.py
        test_workload_olap.py
        test_workload_topic.py
        test_workload_log.py
        test_workload_mixed.py
        test_workload_kv.py
    )

    PEERDIR (
        ydb/tests/olap/load/lib
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
            ydb/tests/tools/nemesis/driver
        )
    ENDIF()

END()
