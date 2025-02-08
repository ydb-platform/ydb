LIBRARY()

PEERDIR(
    util
    library/cpp/deprecated/atomic
    library/cpp/containers/absl_flat_hash
    library/cpp/monlib/service/pages
    library/cpp/threading/hot_swap
)

GENERATE_ENUM_SERIALIZATION_WITH_HEADER(defs.h)

SRCS(
    defs.h
    immediate_control_board_control.cpp
    immediate_control_board_control.h
    immediate_control_board_html_renderer.cpp
    immediate_control_board_impl.cpp
    immediate_control_board_impl.h
    immediate_control_board_wrapper.h
    static_control_board_impl.cpp
    static_control_board_impl.h
)

END()

RECURSE_FOR_TESTS(
    ut
)
