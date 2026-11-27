LIBRARY()

SRCS(
    cluster_discovery_service.cpp
    cluster_discovery_worker.cpp
    counters.cpp
    grpc_service.cpp
)

PEERDIR(
    library/cpp/monlib/dynamic_counters
    ydb/core/base
    ydb/core/grpc_services
    ydb/core/grpc_services/base
    ydb/core/mind/address_classification
    ydb/core/mon
    ydb/core/persqueue/public/cluster_tracker
    ydb/core/protos
    ydb/core/util
    ydb/library/actors/core
    ydb/library/grpc/server
    ydb/public/api/grpc/draft
    ydb/services/persqueue_cluster_discovery/cluster_ordering
)

END()

RECURSE(
    cluster_ordering
)

RECURSE_FOR_TESTS(
    ut
)
