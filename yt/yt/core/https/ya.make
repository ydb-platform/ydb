LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    config.cpp
    client.cpp
    server.cpp
)

PEERDIR(
    yt/yt/core
    yt/yt/core/http
    library/cpp/http/io
)

END()
