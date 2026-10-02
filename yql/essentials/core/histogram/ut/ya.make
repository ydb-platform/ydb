UNITTEST_FOR(yql/essentials/core/histogram)

SIZE(MEDIUM)

PEERDIR(
    yql/essentials/core/histogram/proto
)

SRCS(
    eq_width_histogram_ut.cpp
    eq_height_histogram_ut.cpp
    eq_height_histogram_test_api.h
)

END()
