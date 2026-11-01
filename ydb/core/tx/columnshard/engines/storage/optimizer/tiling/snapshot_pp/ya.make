LIBRARY()

SRCS(
    GLOBAL wrapper.cpp
    settings.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    library/cpp/json
    ydb/core/base
    ydb/core/formats/arrow
    ydb/core/protos
    ydb/core/tx/columnshard/blobs_action/abstract
    ydb/core/tx/columnshard/common
    ydb/core/tx/columnshard/engines/changes/abstract
    ydb/core/tx/columnshard/engines/portions
    ydb/core/tx/columnshard/engines/scheme
    ydb/core/tx/columnshard/engines/storage/optimizer/abstract
    ydb/core/tx/columnshard/engines/storage/optimizer/lbuckets/planner
    ydb/core/tx/columnshard/engines/storage/optimizer/tiling
    ydb/library/conclusion
    ydb/library/intersection_tree
)

END()
