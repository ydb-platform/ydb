LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    service.cpp
)

PEERDIR(
    yt/yt/core
    library/cpp/testing/gtest
)

END()
