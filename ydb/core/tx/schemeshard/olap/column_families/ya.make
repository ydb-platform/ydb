LIBRARY()

SRCS(
    update.cpp
    schema.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/core/formats/arrow/dictionary
    ydb/core/formats/arrow/serializer
    ydb/core/tx/schemeshard/olap/common
)

YQL_LAST_ABI_VERSION()

END()
