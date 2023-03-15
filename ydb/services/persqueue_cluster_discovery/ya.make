LIBRARY()

SRCS(
    cluster_discovery_service.cpp
    cluster_discovery_worker.cpp
    counters.cpp
    grpc_service.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/client/server
    ydb/core/grpc_services
    ydb/core/mind/address_classification
    ydb/core/mon
    ydb/core/persqueue
    ydb/core/protos
    ydb/core/util
    ydb/public/api/grpc
    ydb/public/api/grpc/draft
    ydb/public/api/protos
    ydb/services/persqueue_cluster_discovery/cluster_ordering
)

END()

RECURSE_FOR_TESTS(
    ut
)
