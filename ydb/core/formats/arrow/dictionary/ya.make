LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/protos
    ydb/core/formats/arrow/simple_builder
    ydb/core/formats/arrow/switch
    ydb/library/actors/core
)

SRCS(
    conversion.cpp
    object.cpp
    diff.cpp
)

END()
