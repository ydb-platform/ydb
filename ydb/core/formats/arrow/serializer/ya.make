LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/formats/arrow/common
    ydb/library/actors/core
    ydb/core/protos
)

SRCS(
    abstract.cpp
    GLOBAL arrow.cpp
    stream.cpp
    parsing.cpp
)

END()
