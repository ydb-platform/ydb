UNITTEST_FOR(ydb/core/tx/replication/controller)

FORK_SUBTESTS()

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

TIMEOUT(600)

PEERDIR(
    ydb/core/tx/replication/ut_helpers
    library/cpp/testing/unittest
)

SRCS(
    dst_creator_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
