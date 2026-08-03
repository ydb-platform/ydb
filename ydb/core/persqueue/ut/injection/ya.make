UNITTEST_FOR(ydb/core/persqueue)

YQL_LAST_ABI_VERSION()

SIZE(MEDIUM)

FORK_SUBTESTS()
SPLIT_FACTOR(2)
REQUIREMENTS(cpu:2)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/persqueue/ut/common
    ydb/core/testlib/default
)

SRCS(
    distributed_three_pq_tx_ut.cpp
)

END()
