LIBRARY()

SRCS(
    optimizer.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/storage/optimizer/sbuckets/counters
    ydb/core/tx/columnshard/engines/portions
    ydb/core/tx/columnshard/hooks/abstract
)

END()
