LIBRARY()

SRCS(
    schema.cpp
    update.cpp
    validator.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/protos
    ydb/core/tx/columnshard/engines/storage/indexes/max
)

YQL_LAST_ABI_VERSION()

END()
