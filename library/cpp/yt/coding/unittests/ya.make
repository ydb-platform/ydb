GTEST()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SRCS(
    zig_zag_ut.cpp
    varint_ut.cpp
)

PEERDIR(
    library/cpp/yt/coding
    library/cpp/testing/gtest
)

END()
