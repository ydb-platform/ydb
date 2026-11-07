LIBRARY()

SRCS(
    abstract.cpp
    events.cpp
)

PEERDIR(
    ydb/core/formats/arrow/reader/stats
    ydb/library/actors/core
    ydb/core/protos
)

END()
