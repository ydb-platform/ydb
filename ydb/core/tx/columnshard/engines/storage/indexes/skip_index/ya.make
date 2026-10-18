LIBRARY()


SRCS(
    meta.cpp
    constructor.cpp
    index_info_ext.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/portions
    ydb/core/tx/columnshard/engines/scheme
    ydb/core/tx/columnshard/engines/scheme/abstract
    ydb/core/tx/columnshard/engines/scheme/indexes/abstract
    ydb/core/tx/columnshard/engines/storage/indexes/bits_storage
)

END()
