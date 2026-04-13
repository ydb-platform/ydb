LIBRARY()

SRCS(
    GLOBAL tiling.cpp
    GLOBAL wrapper.cpp
    counters.cpp
    levels.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    library/cpp/json
    ydb/core/protos
    ydb/core/formats/arrow
    ydb/core/tx/columnshard/blobs_action/abstract
    ydb/core/tx/columnshard/engines/changes
    ydb/core/tx/columnshard/engines/changes/abstract
    ydb/core/tx/columnshard/engines/scheme
    ydb/core/tx/columnshard/engines/storage/optimizer/lbuckets/planner
    ydb/library/intersection_tree
)

END()
