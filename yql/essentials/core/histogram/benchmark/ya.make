NO_BUILD_IF(SANITIZER_TYPE)

G_BENCHMARK()

SRCS(
    eq_height_histogram_bench.cpp
)

PEERDIR(
    yql/essentials/core/histogram
    yql/essentials/core/histogram/test_support
)

END()
