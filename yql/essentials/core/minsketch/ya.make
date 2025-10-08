LIBRARY()

ENABLE(YQL_STYLE_CPP)

SRCS(
    count_min_sketch.h
    count_min_sketch.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
