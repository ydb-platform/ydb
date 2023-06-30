LIBRARY()

SRCS(
    client.cpp
    config.cpp
    connection.cpp
)

PEERDIR(
    library/cpp/yt/string
    yt/yt/core
    yt/yt/client
)

END()

RECURSE_FOR_TESTS(unittests)
