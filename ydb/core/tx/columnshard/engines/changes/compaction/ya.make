LIBRARY()

SRCS(
    merger.cpp
    sort_merger.cpp
)

PEERDIR(
    ydb/core/formats/arrow/filter
    contrib/libs/apache/arrow
    contrib/libs/apache/arrow_next
    ydb/core/formats/arrow
    ydb/core/tx/tiering
    ydb/core/tx/columnshard/engines/changes/compaction/abstract
    ydb/core/tx/columnshard/engines/changes/compaction/common
    ydb/core/tx/columnshard/engines/changes/compaction/plain
    ydb/core/tx/columnshard/engines/changes/compaction/sparsed
    ydb/core/tx/columnshard/engines/changes/compaction/dictionary
    ydb/core/tx/columnshard/engines/changes/compaction/sub_columns
)

END()
