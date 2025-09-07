LIBRARY()

SRCS(
    mirrorer.cpp
    offload_actor.cpp
    partition.cpp
    partition_blob_encoder.cpp
    partition_compactification.cpp
    partition_compaction.cpp
    partition_init.cpp
    partition_monitoring.cpp
    partition_read.cpp
    partition_sourcemanager.cpp
    partition_write.cpp
    sourceid.cpp
    user_info.cpp
)



PEERDIR(
    contrib/libs/fmt
    ydb/library/actors/core
    ydb/core/base
    ydb/core/engine/minikql
    ydb/core/persqueue/events
    ydb/core/persqueue/common
    ydb/core/persqueue/partition_key_range
    ydb/core/persqueue/pqtablet/blob
    ydb/core/persqueue/pqtablet/common
    ydb/library/logger
)

END()

RECURSE_FOR_TESTS(
)
