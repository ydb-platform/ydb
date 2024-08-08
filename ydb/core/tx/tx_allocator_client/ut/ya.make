UNITTEST_FOR(ydb/core/tx/tx_allocator_client)

FORK_SUBTESTS()

TIMEOUT(600)

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/mind
    ydb/core/testlib/default
    ydb/core/tx
    ydb/core/tx/tx_allocator_client
)

YQL_LAST_ABI_VERSION()

SRCS(
    actor_client_ut.cpp
    ut_helpers.cpp
)

END()
