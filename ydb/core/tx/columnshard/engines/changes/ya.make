LIBRARY()

SRCS(
    cleanup_portions.cpp
    cleanup_tables.cpp
    compaction.cpp
    general_compaction.cpp
    indexation.cpp
    merge_subset.cpp
    ttl.cpp
    with_appended.cpp
)

PEERDIR(
    ydb/core/formats/arrow
    ydb/core/tx/columnshard/common
    ydb/core/tx/columnshard/engines/insert_table
    ydb/core/tx/columnshard/engines/changes/abstract
    ydb/core/tx/columnshard/engines/changes/compaction
    ydb/core/tx/columnshard/engines/changes/counters
    ydb/core/tx/columnshard/engines/changes/actualization
    ydb/core/tx/columnshard/splitter
    ydb/core/tablet_flat
    ydb/core/tx/tiering
    ydb/core/protos
)

END()
