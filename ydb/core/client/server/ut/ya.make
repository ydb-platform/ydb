UNITTEST_FOR(ydb/core/client/server)

FORK_SUBTESTS()

SPLIT_FACTOR(20)

SIZE(MEDIUM)

PEERDIR(
    library/cpp/testing/gmock_in_unittest
    ydb/core/persqueue
    ydb/core/tablet_flat
    ydb/core/testlib/default
    ydb/core/testlib/actors
    ydb/library/grpc/server
    ydb/library/grpc/server/actors
)

YQL_LAST_ABI_VERSION()

SRCS(
    grpc_choose_proxy_ut.cpp
    msgbus_server_pq_metarequest_ut.cpp
)

END()
