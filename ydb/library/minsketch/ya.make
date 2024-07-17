LIBRARY()

SRCS(
    count_min_sketch.h
    count_min_sketch.cpp
    stack_count_min_sketch.h
    stack_count_min_sketch.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
