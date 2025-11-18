GTEST()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SRCS(
    env_ut.cpp
)

PEERDIR(
    library/cpp/yt/misc
    library/cpp/yt/string
    library/cpp/yt/system
    library/cpp/testing/gtest
)

END()
