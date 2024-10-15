LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/formats/arrow/switch
    ydb/core/formats/arrow/reader
    ydb/library/actors/core
    ydb/library/services
    ydb/library/actors/protos
    ydb/library/formats/arrow/hash
    ydb/library/formats/arrow/common
    ydb/library/formats/arrow/simple_builder
)

SRCS(
    calcer.cpp
)

END()

