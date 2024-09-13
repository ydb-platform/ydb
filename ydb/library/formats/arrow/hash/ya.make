LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    ydb/library/formats/arrow/simple_builder
    ydb/library/formats/arrow/switch
    ydb/library/actors/core
    ydb/library/services
    ydb/library/actors/protos
)

SRCS(
    xx_hash.cpp
)

END()

