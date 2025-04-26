LIBRARY()

SRCS(
    events.cpp
    config.cpp
    abstract.cpp
    service.cpp
)

PEERDIR(
    ydb/core/kqp/runtime
    ydb/library/actors/core
    ydb/services/metadata/request
)

END()
