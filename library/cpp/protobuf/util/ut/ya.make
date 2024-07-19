
UNITTEST_FOR(library/cpp/protobuf/util)

SRCS(
    extensions.proto
    sample_for_is_equal.proto
    sample_for_simple_reflection.proto
    common_ut.proto
    pb_io_ut.cpp
    is_equal_ut.cpp
    iterators_ut.cpp
    simple_reflection_ut.cpp
    repeated_field_utils_ut.cpp
    walk_ut.cpp
    merge_ut.cpp
)

END()
