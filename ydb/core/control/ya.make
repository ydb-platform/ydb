LIBRARY()

PEERDIR(
    ydb/library/actors/core
    library/cpp/monlib/dynamic_counters
    util
    ydb/core/base
    ydb/core/mon
    ydb/core/node_whiteboard
    library/cpp/deprecated/atomic
)

SRCS(
    defs.h
    immediate_control_board_actor.cpp
    immediate_control_board_actor.h
    immediate_control_board_control.cpp
    immediate_control_board_control.h
    immediate_control_board_impl.cpp
    immediate_control_board_impl.h
    immediate_control_board_wrapper.h
)

END()

RECURSE_FOR_TESTS(
    ut
)
