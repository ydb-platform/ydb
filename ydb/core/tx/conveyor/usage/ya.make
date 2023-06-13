LIBRARY()

SRCS(
    events.cpp
    config.cpp
    abstract.cpp
    service.cpp
)

PEERDIR(
    library/cpp/actors/core
    ydb/services/metadata/request
)

END()
