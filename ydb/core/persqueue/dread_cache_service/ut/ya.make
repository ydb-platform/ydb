UNITTEST_FOR(ydb/core/persqueue)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    ydb/core/persqueue/ut/common
    ydb/core/testlib/default
    ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils
)

YQL_LAST_ABI_VERSION()

SRCS(
    caching_proxy_ut.cpp
)

# RESOURCE(
# )

END()
