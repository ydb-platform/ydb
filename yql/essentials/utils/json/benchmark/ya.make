NO_BUILD_IF(SANITIZER_TYPE)

G_BENCHMARK()

SRCS(
    benchmark.cpp
)

PEERDIR(
    yql/essentials/utils/json
)

END()
