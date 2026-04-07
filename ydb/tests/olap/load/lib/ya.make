PY3_LIBRARY()

    PY_SRCS (
        conftest.py
        clickbench.py
        external.py
        import_csv.py
        tpcc.py
        tpcds.py
        tpch.py
        workload_executor.py
        workload_executor_parallel.py
        workload_manager.py
        workload_simple_queue.py
        workload_oltp.py
        workload_olap.py
        workload_topic.py
        workload_kv.py
        workload_log.py
        workload_mixed.py
        workload_ctas.py
        workload_transfer.py
        workload_kafka.py
        workload_topic_kafka.py
        workload_node_broker.py
        workload_reconfig_state_storage.py
        workload_show_create.py
        workload_cdc.py
        workload_statistics.py
        upload.py

    )

    PEERDIR (
        contrib/python/allure-pytest
        contrib/python/allure-python-commons
        contrib/python/matplotlib
        contrib/python/pytest-timeout
        ydb/public/sdk/python/enable_v3_new_behavior
        ydb/tests/olap/lib
        ydb/tests/olap/scenario/helpers
        library/python/testing/yatest_common
        ydb/public/sdk/python
    )

END()
