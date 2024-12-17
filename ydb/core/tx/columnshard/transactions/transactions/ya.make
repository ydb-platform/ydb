LIBRARY()

SRCS(
    tx_add_sharding_info.cpp
    tx_finish_async.cpp
)

PEERDIR(
    ydb/services/metadata/abstract
    ydb/core/tx/columnshard/blobs_action/protos
    ydb/core/tx/columnshard/data_sharing/protos
)

END()
