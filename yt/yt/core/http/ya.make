LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    client.cpp
    compression.cpp
    config.cpp
    connection_pool.cpp
    connection_reuse_helpers.cpp
    helpers.cpp
    http.cpp
    retrying_client.cpp
    server.cpp
    stream.cpp
)

IF (OPENSOURCE)
    SRCS(compression_opensource.cpp)

    PEERDIR(
        library/cpp/blockcodecs/core
    )
ELSE()
    INCLUDE(ya_non_opensource.inc)
ENDIF()

PEERDIR(
    yt/yt/core
    contrib/restricted/http-parser
)

END()
