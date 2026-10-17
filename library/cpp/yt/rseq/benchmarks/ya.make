G_BENCHMARK()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/rseq/supported_arch.inc)

SRCS(
    cpu_id.cpp
)

PEERDIR(
    library/cpp/yt/rseq
)

END()
