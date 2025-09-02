PY3_LIBRARY()

    PY_SRCS (
        allure_utils.py
        results_processor.py
        remote_execution.py
        ydb_cluster.py
        utils.py
        ydb_cli.py
    )

    PEERDIR(
        contrib/python/allure-pytest
        contrib/python/allure-python-commons
        contrib/python/PyYAML
        contrib/python/pytz
        contrib/python/requests
        library/python/testing/yatest_common
        library/python/svn_version
        ydb/public/api/client/yc_public/iam
        ydb/tests/oss/ydb_sdk_import
    )

END()
