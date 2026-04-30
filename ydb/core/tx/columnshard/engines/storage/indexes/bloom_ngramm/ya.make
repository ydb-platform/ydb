LIBRARY()

SRCS(
    GLOBAL constructor.cpp
    GLOBAL meta.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/core/formats/arrow/hash
    ydb/core/local_indexes/bloom
    ydb/core/tx/columnshard/engines/storage/indexes/portions
    ydb/core/tx/columnshard/engines/storage/indexes/helper
    ydb/core/tx/columnshard/engines/storage/indexes/skip_index
    ydb/library/conclusion
)

END()
