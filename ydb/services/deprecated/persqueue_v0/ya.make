LIBRARY()

SRCS(
    grpc_pq_clusters_updater_actor.cpp
    grpc_pq_read.cpp
    grpc_pq_read_actor.cpp
    grpc_pq_write.cpp
    grpc_pq_write_actor.cpp
    persqueue.cpp
)

PEERDIR(
    ydb/services/deprecated/persqueue_v0/api/grpc
    ydb/services/deprecated/persqueue_v0/api/protos
    ydb/library/persqueue/deprecated/read_batch_converter
    ydb/core/base
    ydb/core/tx/tx_proxy
    ydb/core/client/server
    ydb/core/grpc_services
    ydb/core/mind/address_classification
    ydb/core/persqueue
    ydb/core/persqueue/events
    ydb/core/persqueue/writer
    ydb/core/protos
    ydb/library/aclib
    ydb/library/persqueue/topic_parser
    ydb/services/lib/actors
    ydb/services/lib/sharding
    ydb/services/persqueue_v1
    ydb/services/metadata
)

END()
