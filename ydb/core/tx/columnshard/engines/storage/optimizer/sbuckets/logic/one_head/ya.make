LIBRARY()

SRCS(
    logic.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/storage/optimizer/sbuckets/logic/abstract
    ydb/core/tx/columnshard/engines/changes
    ydb/core/tx/columnshard/common
)

END()
