LIBRARY()

SRCS(
    tx_draft.cpp
    tx_write.cpp
    tx_write_index.cpp
    tx_gc_insert_table.cpp
    tx_gc_indexed.cpp
    tx_remove_blobs.cpp
)

PEERDIR(
    ydb/core/protos
    contrib/libs/apache/arrow
    ydb/core/tablet_flat
    ydb/core/tx/tiering
    ydb/core/tx/columnshard/data_sharing/protos
    ydb/core/tx/columnshard/blobs_action/events
)

END()
