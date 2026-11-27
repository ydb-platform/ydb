LIBRARY()

SRCS(
    grpc_pq_read.cpp
    grpc_pq_read.h
    grpc_pq_schema.cpp
    grpc_pq_schema.h
    grpc_pq_write.cpp
    grpc_pq_write.h
    persqueue.cpp
    persqueue.h
    services_initializer.cpp
    topic.cpp
    topic_deferred_publish.cpp
)

PEERDIR(
    library/cpp/monlib/dynamic_counters
    ydb/core/base
    ydb/core/grpc_services
    ydb/core/grpc_services/base
    ydb/core/persqueue/common
    ydb/core/persqueue/public/cluster_tracker
    ydb/core/protos
    ydb/core/tx/scheme_board
    ydb/library/actors/core
    ydb/library/cloud_permissions
    ydb/library/grpc/server
    ydb/public/api/grpc
    ydb/public/api/grpc/draft
    ydb/public/api/protos
    ydb/services/persqueue_v1/actors
    ydb/services/persqueue_v1/actors/schema/pqv1
    ydb/services/persqueue_v1/actors/schema/topic
)

END()

RECURSE(
    actors
)

RECURSE_FOR_TESTS(
    ut
    ut/new_schemecache_ut
    ut/describes_ut
    ut/direct_read_restore_ut
    ut/tablet_restart_session_ut
)
