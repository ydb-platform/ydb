LIBRARY()

SRCS(
    autopartitioning_manager.cpp
    partitioning_keys_manager.cpp
    consumer_offset_tracker.cpp
    deduplication_write_queue.cpp
    message_id_deduplicator.cpp
    offload_actor.cpp
    ownerinfo.cpp
    partition.cpp
    partition_blob_encoder.cpp
    partition_compactification.cpp
    partition_compaction.cpp
    partition_init.cpp
    partition_mlp.cpp
    partition_monitoring.cpp
    partition_read.cpp
    partition_sourcemanager.cpp
    partition_write.cpp
    sourceid.cpp
    subscriber.cpp
    user_info.cpp
)



PEERDIR(
    library/cpp/containers/absl_flat_hash
    library/cpp/sliding_window
    ydb/core/backup/impl
    ydb/core/base
    ydb/core/jaeger_tracing
    ydb/core/keyvalue
    ydb/core/persqueue/common
    ydb/core/persqueue/events
    ydb/core/persqueue/pqtablet/blob
    ydb/core/persqueue/pqtablet/cache
    ydb/core/persqueue/pqtablet/common
    ydb/core/persqueue/pqtablet/partition/mirrorer
    ydb/core/persqueue/pqtablet/partition/mlp
    ydb/core/persqueue/pqtablet/quota
    ydb/core/persqueue/public
    ydb/core/persqueue/public/counters
    ydb/core/persqueue/public/write_meta
    ydb/core/protos
    ydb/library/actors/core
    ydb/library/kll_median
    ydb/library/persqueue/counter_time_keeper
    ydb/library/persqueue/topic_parser
)

GENERATE_ENUM_SERIALIZATION(deduplication_write_queue.h)

END()

RECURSE(
    mirrorer
    mlp
)

RECURSE_FOR_TESTS(
    ut
)
