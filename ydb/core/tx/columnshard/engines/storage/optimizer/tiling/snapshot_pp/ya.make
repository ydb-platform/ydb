LIBRARY()

SRCS(
    settings.cpp
)

PEERDIR(
    library/cpp/json
    ydb/core/tx/columnshard/common
    ydb/core/tx/columnshard/engines/portions
    ydb/core/tx/columnshard/engines/storage/optimizer/abstract
    ydb/core/tx/columnshard/engines/storage/optimizer/tiling
    ydb/library/conclusion
    ydb/library/intersection_tree
)

END()
