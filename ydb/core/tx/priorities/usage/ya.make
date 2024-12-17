LIBRARY()

SRCS(
    abstract.cpp
    events.cpp
    config.cpp
    service.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/core/protos
)

END()
