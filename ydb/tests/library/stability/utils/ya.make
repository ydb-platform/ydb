PY3_LIBRARY()

    PY_SRCS (
        results_processor.py
        remote_execution.py
        collect_errors.py
        utils.py
        upload_results.py
        results_models.py
    )

    PEERDIR (
        contrib/python/allure-pytest
        contrib/python/allure-python-commons
        contrib/python/matplotlib
        contrib/python/pytest-timeout
        ydb/public/sdk/python/enable_v3_new_behavior
        ydb/tests/olap/lib
        library/python/testing/yatest_common
        ydb/public/sdk/python
    )

END()
