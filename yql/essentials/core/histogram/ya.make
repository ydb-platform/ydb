LIBRARY()

SRCS(
    eq_width_histogram.h
    eq_width_histogram.cpp
    eq_height_histogram.h
    eq_height_histogram.cpp
    eq_height_histogram_reader.h
    eq_height_histogram_reader.cpp
)

PEERDIR(
    yql/essentials/core/histogram/proto
    yql/essentials/utils
)

END()

RECURSE_FOR_TESTS(
    ut
)
