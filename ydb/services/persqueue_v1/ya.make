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
)

PEERDIR(
    library/cpp/json
    ydb/library/actors/core
    library/cpp/containers/disjoint_interval_tree
    ydb/library/grpc/server
    ydb/core/base
    ydb/core/grpc_services
    ydb/core/kqp/common
    ydb/core/kqp/common/events
    ydb/core/kqp/common/simple
    ydb/core/persqueue/events
    ydb/core/persqueue/public/codecs
    ydb/core/persqueue/writer
    ydb/core/protos
    ydb/core/ydb_convert
    ydb/library/aclib
    ydb/public/sdk/cpp/src/library/persqueue/obfuscate
#    ydb/library/persqueue/tests
    ydb/library/persqueue/topic_parser
    ydb/library/cloud_permissions
    ydb/public/api/grpc
    ydb/public/api/grpc/draft
    ydb/public/api/protos
    ydb/services/lib/actors
    ydb/services/lib/sharding
    ydb/services/persqueue_v1/actors
)

END()

RECURSE(
    actors
)

RECURSE_FOR_TESTS(
    ut
    ut/new_schemecache_ut
    ut/describes_ut
)
