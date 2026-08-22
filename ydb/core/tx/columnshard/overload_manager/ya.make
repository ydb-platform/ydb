LIBRARY()

SRCS(
    overload_manager_actor.cpp
    overload_manager_service.cpp
    overload_subscribers.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/protos
    ydb/core/tx/columnshard/flow_control_manager
    ydb/core/tx/data_events
    ydb/library/actors/interconnect
)

END()
