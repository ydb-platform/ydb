LIBRARY()

SRCS(
    cache.cpp
    cache_base.cpp
    config.cpp
    rpc.cpp
)

PEERDIR(
    yt/yt/core
    yt/yt/client
)

END()

RECURSE_FOR_TESTS(
    unittests
)
