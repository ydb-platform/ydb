LIBRARY()

SRCS(
    flow_control_manager_actor.cpp
    flow_control_manager_service.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/core/tx/data_events
)

END()
