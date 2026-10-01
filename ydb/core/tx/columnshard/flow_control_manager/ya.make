LIBRARY()

SRCS(
    delayed_reject_queue.cpp
    drain_rate_controller.cpp
    flow_control_manager_actor.cpp
    flow_control_manager_service.cpp
    node_state_map.cpp
    rate_bucket.cpp
    wait_queue.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/core/tx/data_events
)

END()

RECURSE_FOR_TESTS(
    ut
)
