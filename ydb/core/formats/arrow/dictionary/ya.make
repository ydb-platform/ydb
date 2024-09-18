LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/protos
    ydb/core/formats/arrow/switch
    ydb/library/actors/core
    ydb/library/formats/arrow/transformer
    ydb/library/formats/arrow/common
    ydb/library/formats/arrow/simple_builder
)

SRCS(
    conversion.cpp
    object.cpp
    diff.cpp
)

END()
