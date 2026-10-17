LIBRARY()

SRCS(
    tx_draft.cpp  # gn: into ydb/core/tx/columnshard
    tx_write_index.cpp  # gn: into ydb/core/tx/columnshard
    tx_gc_indexed.cpp  # gn: into ydb/core/tx/columnshard
    tx_remove_blobs.cpp  # gn: into ydb/core/tx/columnshard
    tx_blobs_written.cpp  # gn: into ydb/core/tx/columnshard
)

PEERDIR(
    ydb/core/protos
    contrib/libs/apache/arrow
    ydb/core/tablet_flat
    ydb/core/tx/tiering
    ydb/core/tx/columnshard/data_sharing/protos
    ydb/core/tx/columnshard/blobs_action/events
    ydb/core/tx/columnshard/tracing
)

END()
