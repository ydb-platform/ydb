G_BENCHMARK()

SIZE(MEDIUM)

IF (AUTOCHECK)
    BENCHMARK_OPTS(
        --benchmark_min_time=0.05s
    )
ENDIF()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SRCS(
    compact_vector.cpp
)

PEERDIR(
    library/cpp/yt/compact_containers
)

END()
