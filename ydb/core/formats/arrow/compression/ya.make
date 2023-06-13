LIBRARY()

SRCS(
    diff.cpp
    object.cpp
    parsing.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/protos
    ydb/core/formats/arrow
    ydb/library/conclusion
)

END()
