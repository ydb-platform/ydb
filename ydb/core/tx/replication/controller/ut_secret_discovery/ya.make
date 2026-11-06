UNITTEST_FOR(ydb/core/tx/replication/controller)

FORK_SUBTESTS()

SIZE(SMALL)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/testlib/basics
    ydb/core/testlib/pg
    ydb/library/actors/testlib
)

SRCS(
    secret_discovery_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
