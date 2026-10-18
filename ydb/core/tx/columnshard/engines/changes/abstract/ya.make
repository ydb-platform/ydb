LIBRARY()

SRCS(
    abstract.cpp
    compaction_info.cpp
    settings.cpp
    remove_portions.cpp  # gn: into ydb/core/tx/columnshard/engines
    move_portions.cpp  # gn: into ydb/core/tx/columnshard/engines
    changes.cpp
)

PEERDIR(
    ydb/library/signals
    ydb/core/tx/columnshard/engines/changes/counters
    ydb/core/tablet_flat
    yql/essentials/core/expr_nodes
    ydb/core/tx/columnshard/blobs_action
)

GENERATE_ENUM_SERIALIZATION(abstract.h)

END()
