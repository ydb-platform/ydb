LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/formats/arrow/validation
    ydb/services/metadata/abstract
    ydb/library/actors/core
    ydb/core/protos
)

SRCS(
    abstract.cpp
    GLOBAL native.cpp
    stream.cpp
    parsing.cpp
)

END()
