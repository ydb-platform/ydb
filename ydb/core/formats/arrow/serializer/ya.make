LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/formats/arrow/common
    library/cpp/actors/core
    ydb/core/protos
)

SRCS(
    abstract.cpp
    full.cpp
    batch_only.cpp
    stream.cpp
)

END()
