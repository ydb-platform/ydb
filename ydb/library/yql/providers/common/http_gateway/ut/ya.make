UNITTEST_FOR(ydb/library/yql/providers/common/http_gateway)

FORK_SUBTESTS()

SRCS(
    yql_aws_signature_ut.cpp
    yql_dns_gateway_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
)

YQL_LAST_ABI_VERSION()

END()
