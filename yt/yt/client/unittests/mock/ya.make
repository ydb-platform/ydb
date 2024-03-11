LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    client.cpp
)

PEERDIR(
    library/cpp/testing/gtest_extensions
    yt/yt/client
)

END()
