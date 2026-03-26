LIBRARY()

SRCS(
    GLOBAL tiling.cpp
    counters.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/protos
    ydb/core/formats/arrow
    ydb/core/tx/columnshard/blobs_action/abstract
    ydb/core/tx/columnshard/engines/changes
    ydb/core/tx/columnshard/engines/changes/abstract
    ydb/core/tx/columnshard/engines/scheme
    ydb/library/intersection_tree
)

END()
