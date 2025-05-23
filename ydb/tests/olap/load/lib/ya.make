PY3_LIBRARY()

    PY_SRCS (
        conftest.py
        clickbench.py
        external.py
        tpcds.py
        tpch.py
        workload_simple_queue.py

    )

    PEERDIR (
        contrib/python/allure-pytest
        contrib/python/allure-python-commons
        ydb/public/sdk/python/enable_v3_new_behavior
        ydb/tests/olap/lib
        ydb/tests/olap/scenario/helpers
        library/python/testing/yatest_common
        ydb/public/sdk/python
    )

    BUNDLE(
        ydb/tests/stress/simple_queue NAME simple_queue
        ydb/tests/stress/olap_workload NAME olap_workload
        ydb/tests/stress/oltp_workload NAME oltp_workload 
        ydb/tests/stress/statistics_workload NAME statistics_workload
        ydb/tests/tools/nemesis/driver NAME nemesis
        ydb/apps/ydb NAME ydb_cli
    )

    RESOURCE(
        ydb_cli ydb_cli
        simple_queue simple_queue
        olap_workload olap_workload
        oltp_workload oltp_workload
        statistics_workload statistics_workload
        nemesis nemesis
    )

END()
