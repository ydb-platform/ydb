LIBRARY()

SRCS(
    abstract.cpp
    compaction_info.cpp
    settings.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/counters/common
    ydb/core/tablet_flat
    ydb/library/yql/core/expr_nodes
    ydb/core/tx/columnshard/blobs_action
)

GENERATE_ENUM_SERIALIZATION(abstract.h)

END()
