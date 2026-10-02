GTEST()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

PEERDIR(
    library/cpp/yt/assert
)

SRCS(
    assert_ut.cpp
)

END()
