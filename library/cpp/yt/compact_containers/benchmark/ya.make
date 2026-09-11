G_BENCHMARK()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SRCS(
    compact_vector.cpp
)

PEERDIR(
    library/cpp/yt/compact_containers
)

END()
