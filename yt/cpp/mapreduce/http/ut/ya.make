GTEST()

SRCS(
    connection_pool_ut.cpp
    http_ut.cpp
    simple_server.cpp
)

PEERDIR(
    yt/cpp/mapreduce/http
    library/cpp/testing/common
)

END()
