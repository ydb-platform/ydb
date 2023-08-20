LIBRARY()

SRCS(
    compaction.cpp
    in_granule_compaction.cpp
    split_compaction.cpp
    ttl.cpp
    indexation.cpp
    cleanup.cpp
    mark_granules.cpp
    with_appended.cpp
)

PEERDIR(
    ydb/core/formats/arrow
    ydb/core/tx/columnshard/common
    ydb/core/tx/columnshard/engines/insert_table
    ydb/core/tx/columnshard/engines/changes/abstract
    ydb/core/tx/columnshard/splitter
    ydb/core/tablet_flat
    ydb/core/tx/tiering
    ydb/core/protos
)

END()
