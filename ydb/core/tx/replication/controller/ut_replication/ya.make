UNITTEST_FOR(ydb/core/tx/replication/controller)

FORK_SUBTESTS()

SIZE(SMALL)
REQUIREMENTS(cpu:1)

TIMEOUT(60)

PEERDIR(
    ydb/core/protos
    ydb/core/testlib/pg
    library/cpp/testing/unittest
)

SRCS(
    replication_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
