LIBRARY()

SRCS(
    granule_data_stats.cpp
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
