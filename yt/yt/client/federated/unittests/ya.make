GTEST(unittester-federated-client)

SRCS(
    client_ut.cpp
    connection_ut.cpp
    cache_ut.cpp
)

PEERDIR(
    library/cpp/iterator
    library/cpp/testing/common
    library/cpp/testing/hook

    yt/yt/library/profiling/solomon

    yt/yt/core

    yt/yt/client/federated
    yt/yt/client/unittests/mock
)

END()
