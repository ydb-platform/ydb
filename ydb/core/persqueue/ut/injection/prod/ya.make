UNITTEST_FOR(ydb/core/persqueue)

YQL_LAST_ABI_VERSION()

SIZE(MEDIUM)

FORK_SUBTESTS()
SPLIT_FACTOR(4)
REQUIREMENTS(cpu:2)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/persqueue/ut/common
    ydb/core/testlib/default
    ydb/core/tx
    ydb/core/tx/schemeshard/ut_helpers
)

SRCS(
    prod_pq_injection_ut.cpp
)

END()
