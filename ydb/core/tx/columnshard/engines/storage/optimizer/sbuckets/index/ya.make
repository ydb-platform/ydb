LIBRARY()

SRCS(
    index.cpp
    bucket.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/storage/optimizer/sbuckets/common
    ydb/core/tx/columnshard/engines/storage/optimizer/sbuckets/logic/abstract
    ydb/core/tx/columnshard/hooks/abstract
    ydb/core/tx/columnshard/engines/storage/optimizer/abstract
)

END()
