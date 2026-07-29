GTEST()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SRCS(
    bit_io_ut.cpp
    interpolative_ut.cpp
    zig_zag_ut.cpp
    varint_ut.cpp
)

PEERDIR(
    library/cpp/yt/coding
    library/cpp/testing/gtest
)

END()
