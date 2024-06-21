LIBRARY()

SRCS(
    compaction.cpp
    ttl.cpp
    indexation.cpp
    cleanup_portions.cpp
    cleanup_tables.cpp
    with_appended.cpp
    general_compaction.cpp
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
