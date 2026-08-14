UNITTEST_FOR(ydb/core/tx/schemeshard)

SIZE(MEDIUM)

PEERDIR(
    ydb/core/testlib/default
    ydb/core/tx/schemeshard/ut_helpers
    ydb/library/aclib
    ydb/library/testlib/service_mocks
    ydb/public/sdk/cpp/src/client/driver
    ydb/public/sdk/cpp/src/client/query
)

YQL_LAST_ABI_VERSION()

SRCS(
    ut_external_data_source_iam_token.cpp
)

END()
