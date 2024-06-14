LIBRARY()

SRCS(
    GLOBAL optimizer.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/storage/optimizer/sbuckets/common
    ydb/core/tx/columnshard/engines/storage/optimizer/sbuckets/counters
    ydb/core/tx/columnshard/engines/storage/optimizer/sbuckets/index
    ydb/core/tx/columnshard/engines/storage/optimizer/sbuckets/logic
    ydb/core/tx/columnshard/engines/storage/optimizer/abstract
)

END()
