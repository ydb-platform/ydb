LIBRARY()

SRCS(
    tx_save_cursor.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/core/tx/columnshard/export/protos
    ydb/core/tx/columnshard/blobs_action
    ydb/services/metadata/abstract
)

END()
