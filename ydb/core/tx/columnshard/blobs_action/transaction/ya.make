LIBRARY()

SRCS(
    tx_draft.cpp
    tx_write.cpp
    tx_write_index.cpp
    tx_gc_insert_table.cpp
    tx_gc_indexed.cpp
)

PEERDIR(
    ydb/core/protos
    contrib/libs/apache/arrow
    ydb/core/tablet_flat
    ydb/core/tx/tiering
)

END()
