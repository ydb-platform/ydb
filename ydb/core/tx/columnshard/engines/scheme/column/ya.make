LIBRARY()

SRCS(
    info.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/scheme/abstract

    ydb/core/formats/arrow/dictionary
    ydb/core/formats/arrow/serializer
    ydb/core/formats/arrow/transformer
    ydb/core/formats/arrow/common

    contrib/libs/apache/arrow
)

END()
