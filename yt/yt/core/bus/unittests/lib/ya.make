LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    handlers.cpp
    helpers.cpp
)

PEERDIR(
    yt/yt/core
    yt/yt/core/test_framework
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

END()
