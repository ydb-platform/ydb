LIBRARY()

SRCS(
    GLOBAL constructor.cpp
    GLOBAL meta.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/core/formats/arrow
    ydb/core/tx/columnshard/engines/storage/indexes/portions
    ydb/core/formats/arrow/accessor/abstract
    ydb/core/tx/columnshard/engines/storage/indexes/min_max/misc
)

END()
