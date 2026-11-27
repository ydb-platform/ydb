UNITTEST_FOR(yql/essentials/core/histogram)

SIZE(MEDIUM)

PEERDIR(
    yql/essentials/core/histogram/proto
    yql/essentials/core/histogram/test_support
)

SRCS(
    eq_width_histogram_ut.cpp
    eq_height_histogram_ut.cpp
)

END()
