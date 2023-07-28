GTEST(unittester-containers)

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SRCS(
    sharded_set_ut.cpp
)

PEERDIR(
    library/cpp/yt/containers

    library/cpp/testing/gtest
)

END()
