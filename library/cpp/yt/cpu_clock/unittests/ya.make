GTEST()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SRCS(
    clock_ut.cpp
)

PEERDIR(
    library/cpp/yt/cpu_clock
)

END()
