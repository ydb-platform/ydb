LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    helpers.cpp
)

PEERDIR(
    yt/yt/client
    yt/yt/core/test_framework
)

END()
