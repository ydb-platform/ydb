LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    channel.cpp
    config.cpp
    helpers.cpp
    server.cpp
    GLOBAL backend.cpp
)

PEERDIR(
    yt/yt/core
    yt/yt/core/http
    yt/yt/core/https
    yt/yt/core/rpc/grpc
)

END()

RECURSE_FOR_TESTS(
    unittests
)
