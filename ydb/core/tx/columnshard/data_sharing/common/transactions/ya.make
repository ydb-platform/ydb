LIBRARY()

SRCS(
    tx_extension.cpp
)

PEERDIR(
    ydb/core/tablet_flat
    ydb/core/tx/tiering
    ydb/services/metadata/abstract
    ydb/core/tx/columnshard/data_sharing/protos
    ydb/core/tx/columnshard/blobs_action/protos
    ydb/core/base
    ydb/core/tx/tiering
)

END()
