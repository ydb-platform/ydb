UNITTEST_FOR(ydb/core/kqp/gateway/behaviour/streaming_query)

FORK_SUBTESTS()
SPLIT_FACTOR(10)
SIZE(MEDIUM)

SRCS(
    scheme_transaction_ut.cpp
)

PEERDIR(
    ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

END()
