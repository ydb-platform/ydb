UNITTEST_FOR(ydb/public/sdk/cpp/src/client/topic)

FORK_SUBTESTS()

SRCS(
    read_session_replication_ut.cpp
)

PEERDIR(
    library/cpp/containers/disjoint_interval_tree
    library/cpp/testing/common
    ydb/public/api/grpc
)

END()
