UNITTEST_FOR(ydb/services/persqueue_cluster_discovery)

FORK_SUBTESTS()

SIZE(MEDIUM)

SRCS(
    cluster_discovery_service_ut.cpp
)

PEERDIR(
    ydb/library/actors/http
    ydb/core/testlib/default
    ydb/public/api/grpc
    ydb/services/persqueue_cluster_discovery
)

YQL_LAST_ABI_VERSION()

END()
