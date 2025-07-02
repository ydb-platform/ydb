LIBRARY()

SRCS(
    abstract.cpp
    events.cpp
    context.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/core/protos
)

END()
