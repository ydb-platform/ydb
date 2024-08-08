UNITTEST_FOR(ydb/core/client/server)

FORK_SUBTESTS()

SPLIT_FACTOR(20)

TIMEOUT(600)

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

PEERDIR(
    library/cpp/testing/gmock_in_unittest
    ydb/core/persqueue
    ydb/core/tablet_flat
    ydb/core/testlib/default
    ydb/core/testlib/actors
)

YQL_LAST_ABI_VERSION()

SRCS(
    msgbus_server_pq_metarequest_ut.cpp
)

END()
