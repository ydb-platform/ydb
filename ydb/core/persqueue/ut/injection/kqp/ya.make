UNITTEST_FOR(ydb/core/persqueue)

ADDINCL(
    ydb/public/sdk/cpp
)

YQL_LAST_ABI_VERSION()

SIZE(LARGE)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
TIMEOUT(900)

FORK_SUBTESTS()
SPLIT_FACTOR(2)
REQUIREMENTS(cpu:2)

PEERDIR(
    library/cpp/testing/unittest
    library/cpp/threading/future
    ydb/core/persqueue/ut/common
    ydb/core/testlib/default
    ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils
    ydb/public/sdk/cpp/src/client/query
    ydb/public/sdk/cpp/src/client/topic
    ydb/public/sdk/cpp/src/client/topic/ut/ut_utils
)

SRCS(
    kqp_topic_tx_spike_ut.cpp
)

END()
