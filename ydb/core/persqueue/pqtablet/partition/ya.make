LIBRARY()

SRCS(
    account_read_quoter.cpp
    autopartitioning_manager.cpp
    offload_actor.cpp
    ownerinfo.cpp
    partition.cpp
    partition_blob_encoder.cpp
    partition_compactification.cpp
    partition_compaction.cpp
    partition_init.cpp
    partition_monitoring.cpp
    partition_read.cpp
    partition_sourcemanager.cpp
    partition_write.cpp
    quota_tracker.cpp
    read_quoter.cpp
    sourceid.cpp
    subscriber.cpp
    user_info.cpp
    write_quoter.cpp
)



PEERDIR(
    ydb/core/backup/impl
    ydb/core/persqueue/events
    ydb/core/persqueue/common
    ydb/core/persqueue/public/counters
    ydb/core/persqueue/public/write_meta
    ydb/core/persqueue/pqtablet/blob
    ydb/core/persqueue/pqtablet/cache
    ydb/core/persqueue/pqtablet/common
    ydb/core/persqueue/pqtablet/partition/mirrorer
)

END()

RECURSE(
    mirrorer
)

RECURSE_FOR_TESTS(
)
