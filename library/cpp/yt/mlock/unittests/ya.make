GTEST()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SRCS(
    mlock_ut.cpp
)

PEERDIR(
    library/cpp/yt/mlock
)

END()
