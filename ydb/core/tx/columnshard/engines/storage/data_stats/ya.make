LIBRARY()

SRCS(
    data_stats_by_storage.cpp
    shard_data_stats.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/protos
    ydb/core/tx/columnshard/blobs_action/abstract
    ydb/core/tx/columnshard/common
    ydb/core/tx/columnshard/counters
    ydb/core/tx/columnshard/engines/portions
)

END()
