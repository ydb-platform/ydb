GTEST()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SRCS(
    per_cpu_ut.cpp
)

PEERDIR(
    library/cpp/yt/memory
    library/cpp/yt/rseq
)

END()
