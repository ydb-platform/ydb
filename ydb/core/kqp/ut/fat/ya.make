UNITTEST_FOR(ydb/core/kqp)

OWNER(
    g:kikimr
)

FORK_SUBTESTS()

TIMEOUT(2400)
TAG(ya:fat)
SIZE(LARGE)
SPLIT_FACTOR(5)

SRCS(
    kqp_force_newengine_ut.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/client/ydb_proto
    ydb/core/kqp
    ydb/core/kqp/counters
    ydb/core/kqp/host
    ydb/core/kqp/provider
    ydb/core/kqp/ut/common
)

YQL_LAST_ABI_VERSION()

END()
