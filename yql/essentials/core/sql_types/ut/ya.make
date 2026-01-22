UNITTEST_FOR(yql/essentials/core/sql_types)

SRCS(
    match_recognize_ut.cpp
    normalize_name_ut.cpp
    window_frame_bounds_ut.cpp
    window_number_and_direction_ut.cpp
)

PEERDIR(
    yql/essentials/core/sql_types
)

SIZE(SMALL)

END()
