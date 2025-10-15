LIBRARY()

SRCS(
    merger.cpp
)

PEERDIR(
    ydb/core/tx/tiering
    ydb/core/tx/columnshard/engines/changes/compaction/abstract
    ydb/core/tx/columnshard/engines/changes/compaction/common
    ydb/core/tx/columnshard/engines/changes/compaction/plain
    ydb/core/tx/columnshard/engines/changes/compaction/sparsed
    ydb/core/tx/columnshard/engines/changes/compaction/dictionary
    ydb/core/tx/columnshard/engines/changes/compaction/sub_columns
)

END()
