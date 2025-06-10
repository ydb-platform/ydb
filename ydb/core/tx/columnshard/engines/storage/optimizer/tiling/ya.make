LIBRARY()

SRCS(
    GLOBAL tiling.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/protos
    ydb/core/formats/arrow
    ydb/core/tx/columnshard/engines/changes/abstract
    ydb/library/intersection_tree
)

END()
