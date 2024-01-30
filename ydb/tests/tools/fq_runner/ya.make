PY3_LIBRARY()

PY_SRCS(
    custom_hooks.py
    fq_client.py
    kikimr_metrics.py
    kikimr_runner.py
    kikimr_utils.py
    mvp_mock.py
)

PEERDIR(
    contrib/python/requests
    library/python/retry
    library/python/testing/yatest_common
    ydb/library/yql/providers/common/proto
    ydb/public/api/grpc/draft
    ydb/tests/library
)

END()
