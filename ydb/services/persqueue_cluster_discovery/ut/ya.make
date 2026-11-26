UNITTEST_FOR(ydb/services/persqueue_cluster_discovery)

FORK_SUBTESTS()

SIZE(MEDIUM)

SRCS(
    cluster_discovery_service_ut.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/kqp/common
    ydb/core/mind/address_classification
    ydb/core/persqueue/public/cluster_tracker
    ydb/core/testlib/default
    ydb/library/actors/http
    ydb/public/api/grpc
    ydb/public/api/grpc/draft
)

YQL_LAST_ABI_VERSION()

END()
