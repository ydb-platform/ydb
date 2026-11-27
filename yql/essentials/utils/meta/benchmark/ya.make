NO_BUILD_IF(SANITIZER_TYPE)

G_BENCHMARK()

PEERDIR(
    yql/essentials/utils/meta
)

SRCS(
    benchmark.cpp
)

END()
