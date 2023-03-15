LIBRARY()

SRCS(
    yql_http_gateway.cpp
    yql_http_default_retry_policy.cpp
)

PEERDIR(
    contrib/libs/curl
    library/cpp/actors/prof
    library/cpp/monlib/dynamic_counters
    library/cpp/retry
    ydb/library/yql/providers/common/proto
    ydb/library/yql/public/issue
    ydb/library/yql/utils/log
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    mock
)

RECURSE_FOR_TESTS(
    ut
)
