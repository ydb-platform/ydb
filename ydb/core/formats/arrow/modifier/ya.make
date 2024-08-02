LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    ydb/library/conclusion
    ydb/core/formats/arrow/switch
    ydb/core/formats/arrow/protos
    ydb/library/actors/core
)

SRCS(
    schema.cpp
    subset.cpp
)

END()
