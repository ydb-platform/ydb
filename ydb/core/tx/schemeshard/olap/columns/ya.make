YQL_LIBRARY()

SRCS(
    schema.cpp
    update.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/core/scheme_types
    ydb/core/formats/arrow/dictionary
    ydb/core/formats/arrow/serializer
    ydb/core/tx/schemeshard/olap/common
    ydb/core/tx/columnshard/engines/scheme/defaults/common
)

END()
