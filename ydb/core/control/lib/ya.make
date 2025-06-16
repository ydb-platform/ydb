LIBRARY()

PEERDIR(
    ydb/core/control/lib/base
    ydb/core/control/lib/generated
    util
    library/cpp/containers/absl_flat_hash
    library/cpp/deprecated/atomic
    library/cpp/monlib/service/pages
    library/cpp/threading/hot_swap
)

SRCS(
    defs.h
    dynamic_control_board_impl.cpp
    dynamic_control_board_impl.h
    immediate_control_board_control.h
    immediate_control_board_html_renderer.cpp
    immediate_control_board_impl.cpp
    immediate_control_board_impl.h
    immediate_control_board_wrapper.h
)

END()

RECURSE_FOR_TESTS(
    ut
)
