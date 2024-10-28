LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    ydb/services/metadata/abstract
    ydb/library/actors/core
    ydb/library/formats/arrow/common
    ydb/core/protos
)

SRCS(
    abstract.cpp
    GLOBAL native.cpp
    stream.cpp
    parsing.cpp
)

END()
