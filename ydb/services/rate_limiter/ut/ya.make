UNITTEST_FOR(ydb/services/rate_limiter)

SIZE(MEDIUM)

SRCS(
    rate_limiter_ut.cpp
)

PEERDIR(
    ydb/core/testlib/default
    ydb/public/sdk/cpp/client/ydb_coordination
    ydb/public/sdk/cpp/client/ydb_rate_limiter
)

YQL_LAST_ABI_VERSION()

END()
