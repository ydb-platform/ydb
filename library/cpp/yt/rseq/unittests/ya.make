GTEST()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/rseq/supported_arch.inc)

SRCS(
    cpu_id_ut.cpp
    per_cpu_ut.cpp
)

PEERDIR(
    library/cpp/yt/memory
    library/cpp/yt/rseq
)

END()
