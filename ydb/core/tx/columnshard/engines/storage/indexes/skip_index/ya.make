LIBRARY()


SRCS(
    meta.cpp
    constructor.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/scheme/abstract
    ydb/core/tx/columnshard/engines/scheme/indexes/abstract
    ydb/core/tx/columnshard/engines/storage/indexes/bits_storage
)

END()
