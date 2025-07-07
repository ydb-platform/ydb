LIBRARY()

SRCS(
    GLOBAL optimizer.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/protos
    ydb/core/formats/arrow
    ydb/core/tx/columnshard/engines/changes/abstract
    ydb/core/tx/columnshard/engines/storage/optimizer/lcbuckets/planner/level
    ydb/core/tx/columnshard/engines/storage/optimizer/lcbuckets/planner/selector
)

END()
