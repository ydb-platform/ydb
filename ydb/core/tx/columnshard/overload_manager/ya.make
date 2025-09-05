LIBRARY()

SRCS(
    actor.cpp
    service.cpp
    overload_subscribers.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/core/tx/data_events
)

END()
