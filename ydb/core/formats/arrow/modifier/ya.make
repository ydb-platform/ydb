LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    ydb/library/conclusion
    ydb/core/formats/arrow/switch
    ydb/library/actors/core
)

SRCS(
    schema.cpp
)

END()
