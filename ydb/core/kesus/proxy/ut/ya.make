UNITTEST_FOR(ydb/core/kesus/proxy)

SIZE(MEDIUM)

PEERDIR(
    ydb/core/testlib/default
)

SRCS(
    proxy_actor_ut.cpp
    ut_helpers.cpp
)

YQL_LAST_ABI_VERSION()

END()
