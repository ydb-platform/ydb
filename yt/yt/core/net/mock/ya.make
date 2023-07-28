LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    dialer.cpp
)

PEERDIR(
    library/cpp/testing/gtest
    yt/yt/core
)

END()
