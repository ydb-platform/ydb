LIBRARY()

SRCS(
    abstract.cpp
    mark.cpp
    compaction_info.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/counters/common
    ydb/core/tablet_flat
    ydb/library/yql/core/expr_nodes
)

GENERATE_ENUM_SERIALIZATION(abstract.h)

END()
