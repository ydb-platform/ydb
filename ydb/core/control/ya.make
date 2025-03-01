LIBRARY()

PEERDIR(
    ydb/library/actors/core
    library/cpp/monlib/dynamic_counters
    ydb/core/control/lib
    ydb/core/base
    ydb/core/mon
    ydb/core/node_whiteboard
)

SRCS(
    defs.h
    immediate_control_board_actor.cpp
    immediate_control_board_actor.h
)

END()

RECURSE_FOR_TESTS(
    ut
)
