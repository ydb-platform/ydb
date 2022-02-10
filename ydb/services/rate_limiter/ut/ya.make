UNITTEST_FOR(ydb/services/rate_limiter)

OWNER(
    galaxycrab
    g:kikimr
)

SIZE(MEDIUM)

SRCS(
    rate_limiter_ut.cpp
)

PEERDIR(
    ydb/core/testlib
    ydb/public/sdk/cpp/client/ydb_coordination
    ydb/public/sdk/cpp/client/ydb_rate_limiter
)

YQL_LAST_ABI_VERSION() 
 
END()
