LIBRARY()

SRCS(
    yql_aws_signature.cpp
    yql_http_default_retry_policy.cpp
    yql_http_gateway.cpp
)

PEERDIR(
    contrib/libs/curl
    library/cpp/monlib/dynamic_counters
    library/cpp/retry
    ydb/library/actors/http
    ydb/library/actors/prof
    ydb/library/actors/protos
    ydb/library/yql/providers/common/proto
    ydb/library/yql/public/issue
    ydb/library/yql/utils
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
