LIBRARY()

SRCS(
    tx_add_sharding_info.cpp  # gn: into ydb/core/tx/columnshard
    tx_finish_async.cpp  # gn: into ydb/core/tx/columnshard
)

PEERDIR(
    ydb/services/metadata/abstract
    ydb/core/tx/columnshard/blobs_action/protos
    ydb/core/tx/columnshard/data_sharing/protos
)

END()
