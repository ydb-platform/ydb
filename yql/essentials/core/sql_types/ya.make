LIBRARY()

SRCS(
    block.h
    match_recognize.h
    match_recognize.cpp
    normalize_name.cpp
    simple_types.h
    simple_types.cpp
    yql_atom_enums.h
    yql_callable_names.h
    block.cpp
    hopping.cpp
    hopping.h
    sort_order.cpp
    sort_order.h
    time_order_recover.cpp
    time_order_recover.h
    window_direction.cpp
    window_direction.h
    window_frame_bounds.cpp
    window_frame_bounds.h
    window_frames_collector_params.cpp
    window_frames_collector_params.h
    window_number_and_direction.cpp
    window_number_and_direction.h
    yql_atom_enums.cpp
    yql_callable_names.cpp
)

PEERDIR(
    yql/essentials/core/issue
    yql/essentials/public/decimal
)

GENERATE_ENUM_SERIALIZATION(hopping.h)
GENERATE_ENUM_SERIALIZATION(match_recognize.h)
GENERATE_ENUM_SERIALIZATION(yql_atom_enums.h)

END()

RECURSE_FOR_TESTS(
    ut
)
