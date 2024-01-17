LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/formats/arrow/simple_builder
    ydb/core/formats/arrow/switch
    ydb/library/actors/core
    ydb/library/services
    ydb/library/actors/protos
)

SRCS(
    calcer.cpp
    xx_hash.cpp
)

END()

