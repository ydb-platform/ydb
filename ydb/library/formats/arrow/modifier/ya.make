LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    ydb/library/conclusion
    ydb/library/formats/arrow/switch
    ydb/library/formats/arrow/protos
    ydb/library/actors/core
)

SRCS(
    schema.cpp
    subset.cpp
)

END()
