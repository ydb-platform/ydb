LIBRARY()

SRCS(
    GLOBAL constructor.cpp
    GLOBAL meta.cpp
    const.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/core/formats/arrow
    ydb/core/tx/columnshard/engines/storage/indexes/portions
    ydb/core/tx/columnshard/engines/storage/indexes/helper
    ydb/library/conclusion
)

END()
