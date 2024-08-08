UNITTEST_FOR(ydb/core/tx/tx_allocator)

FORK_SUBTESTS()

TIMEOUT(600)

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/mind
    ydb/core/testlib/default
    ydb/core/tx
    ydb/core/tx/tx_allocator
)

YQL_LAST_ABI_VERSION()

SRCS(
    txallocator_ut.cpp
    txallocator_ut_helpers.cpp
)

END()
