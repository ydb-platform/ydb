LIBRARY()

SRCS(
    accessor.cpp
)

PEERDIR(
    ydb/core/formats/arrow
    ydb/core/tx/columnshard/engines/scheme/indexes/abstract
    ydb/core/tx/columnshard/engines/storage/indexes/bits_storage
    ydb/core/tx/columnshard/engines/storage/indexes/skip_index
)

END()
