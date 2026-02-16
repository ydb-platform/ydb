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
    ENV(CTAS_WORKLOAD_BINARY="ydb/tests/stress/ctas/ctas")
    ENV(KAFKA_WORKLOAD_BINARY="ydb/tests/stress/kafka/kafka_streams_test")
    ENV(NODE_BROKER_WORKLOAD_BINARY="ydb/tests/stress/node_broker/node_broker")
    ENV(TOPIC_KAFKA_WORKLOAD_BINARY="ydb/tests/stress/topic_kafka/workload_topic_kafka")
    ENV(TRANSFER_WORKLOAD_BINARY="ydb/tests/stress/transfer/transfer")
    ENV(RECONFIG_STATE_STORAGE_WORKLOAD_BINARY="ydb/tests/stress/reconfig_state_storage_workload/reconfig_state_storage_workload")
    ENV(SHOW_CREATE_WORKLOAD_BINARY="ydb/tests/stress/show_create/view/show_create_view")
    ENV(CDC_WORKLOAD_BINARY="ydb/tests/stress/cdc/cdc")
    ENV(STATISTICS_WORKLOAD_BINARY="ydb/tests/stress/statistics_workload/statistics_workload")
    ENV(NEMESIS_BINARY="ydb/tests/tools/nemesis/driver/nemesis")

    TEST_SRCS (
        test_clickbench.py
        test_external.py
        test_import_csv.py
        test_tpcc.py
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
        test_workload_ctas.py
        test_workload_kafka.py
        test_workload_node_broker.py
        test_workload_topic_kafka.py
        test_workload_transfer.py
        test_workload_reconfig_state_storage.py
        test_workload_show_create.py
        test_workload_cdc.py
        test_workload_statistics.py
        test_workload_parallel.py
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
            ydb/tests/stress/ctas
            ydb/tests/stress/kafka
            ydb/tests/stress/node_broker
            ydb/tests/stress/topic_kafka
            ydb/tests/stress/transfer
            ydb/tests/stress/reconfig_state_storage_workload
            ydb/tests/stress/show_create/view
            ydb/tests/stress/cdc
            ydb/tests/stress/statistics_workload
            ydb/tests/tools/nemesis/driver
        )
    ENDIF()

END()
