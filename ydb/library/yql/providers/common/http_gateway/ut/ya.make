UNITTEST_FOR(ydb/library/yql/providers/common/http_gateway)

FORK_SUBTESTS()

SRCS(
    yql_aws_signature_ut.cpp
    yql_dns_gateway_ut.cpp
    yql_http_default_retry_policy_ut.cpp
    yql_http_gateway_ut.cpp
)

PEERDIR(
    library/cpp/http/server
    library/cpp/testing/common
    library/cpp/testing/unittest
    library/cpp/threading/future
)

YQL_LAST_ABI_VERSION()

END()
