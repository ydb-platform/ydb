PY3_LIBRARY()

    PY_SRCS (
        conftest.py
        clickbench.py
        external.py
        import_csv.py
        tpcds.py
        tpch.py
        workload_executor.py
        workload_simple_queue.py
        workload_oltp.py
        upload.py

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

END()
