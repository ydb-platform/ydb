LIBRARY()

SRCS(
    abstract.cpp
    mark.cpp
    compaction.cpp
    ttl.cpp
    indexation.cpp
    cleanup.cpp
    compaction_info.cpp
    mark_granules.cpp
    with_appended.cpp
)

PEERDIR(
    ydb/core/formats/arrow
    ydb/core/tx/columnshard/common
    ydb/core/tx/columnshard/engines/insert_table
    ydb/core/tablet_flat
    ydb/core/tx/tiering
    ydb/core/protos
)

GENERATE_ENUM_SERIALIZATION(abstract.h)

END()
