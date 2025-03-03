LIBRARY()

PEERDIR(
    util
    library/cpp/deprecated/atomic
    library/cpp/containers/absl_flat_hash
    library/cpp/monlib/service/pages
)

SRCS(
    defs.h
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
