LIBRARY()

PEERDIR(
    ydb/core/base
    ydb/core/control/lib
    ydb/core/mon
    ydb/library/actors/core
    library/cpp/deprecated/atomic
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
