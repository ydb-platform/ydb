LIBRARY()

PEERDIR(
    library/cpp/monlib/dynamic_counters
    ydb/core/base
    ydb/core/control/lib
    ydb/core/mon
    ydb/core/node_whiteboard
    ydb/library/actors/core
)

SRCS(
    defs.h
    immediate_control_board_actor.cpp
    immediate_control_board_actor.h
    immediate_control_board_impl.h
)

END()

RECURSE_FOR_TESTS(
    ut
)
