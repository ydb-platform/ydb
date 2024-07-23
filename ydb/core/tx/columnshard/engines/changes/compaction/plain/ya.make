LIBRARY()

SRCS(
    column_cursor.cpp
    column_portion_chunk.cpp
    merged_column.cpp
    logic.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/changes/compaction/common
)

END()
