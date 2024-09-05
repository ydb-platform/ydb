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
)

END()
