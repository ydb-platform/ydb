LIBRARY()

SRCS(
    GLOBAL constructor.cpp
    GLOBAL meta.cpp
    GLOBAL checker.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/core/formats/arrow
    ydb/core/tx/columnshard/engines/storage/indexes/portions
)

END()
