LIBRARY()

SRCS(
    overload_manager_actor.cpp
    overload_manager_service.cpp
    overload_subscribers.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/core/tx/data_events
)

END()
