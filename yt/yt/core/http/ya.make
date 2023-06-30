LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    client.cpp
    config.cpp
    connection_pool.cpp
    connection_reuse_helpers.cpp
    http.cpp
    server.cpp
    stream.cpp
    helpers.cpp
)

PEERDIR(
    yt/yt/core
    contrib/restricted/http-parser
)

END()
