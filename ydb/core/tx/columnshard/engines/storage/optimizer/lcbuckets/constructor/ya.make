LIBRARY()

SRCS(
    GLOBAL constructor.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/protos
    ydb/core/formats/arrow
    ydb/core/tx/columnshard/engines/changes/abstract
    ydb/core/tx/columnshard/engines/storage/optimizer/lcbuckets/constructor/selector
    ydb/core/tx/columnshard/engines/storage/optimizer/lcbuckets/constructor/level
)

END()
