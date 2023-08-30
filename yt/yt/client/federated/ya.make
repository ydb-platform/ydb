LIBRARY()

SRCS(
    client.cpp
    config.cpp
    connection.cpp
    cache.cpp
)

PEERDIR(
    library/cpp/yt/string
    yt/yt/core
    yt/yt/client
    yt/yt/client/cache
)

END()

RECURSE_FOR_TESTS(unittests)
