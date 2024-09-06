LIBRARY()


SRCS(
    meta.cpp
)

PEERDIR(
    ydb/core/formats/arrow
    ydb/core/formats/arrow/protos
    ydb/core/tx/columnshard/engines/storage/chunks
    ydb/core/tx/columnshard/engines/scheme/indexes/abstract
    ydb/core/tx/columnshard/engines/portions
)

YQL_LAST_ABI_VERSION()

END()
