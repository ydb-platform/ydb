LIBRARY()

SRCS(
    granule.cpp
    storage.cpp
    portions_index.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/storage/optimizer/abstract
    ydb/core/tx/columnshard/engines/storage/actualizer/index
    ydb/core/tx/columnshard/counters
    ydb/core/tx/columnshard/engines/portions
    ydb/core/tx/columnshard/hooks/abstract
    ydb/core/base
    ydb/core/formats/arrow/reader
    ydb/core/tx/columnshard/engines/storage/optimizer/sbuckets/optimizer
    ydb/core/tx/columnshard/engines/storage/optimizer/lbuckets/planner
)

GENERATE_ENUM_SERIALIZATION(granule.h)

END()
