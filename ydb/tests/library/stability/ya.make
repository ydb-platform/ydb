PY3_LIBRARY()

    PY_SRCS (
        aggregate_results.py
        build_report.py
        deploy.py
        run_stress.py
        upload_results.py
        workload_executor_parallel.py
    )

    PEERDIR (
        contrib/python/allure-pytest
        contrib/python/allure-python-commons
        contrib/python/matplotlib
        contrib/python/pytest-timeout
        ydb/public/sdk/python/enable_v3_new_behavior
        ydb/tests/olap/lib
        ydb/tests/olap/load/lib
        ydb/tests/olap/scenario/helpers
        library/python/testing/yatest_common
        ydb/public/sdk/python
    )

END()
