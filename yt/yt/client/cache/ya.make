LIBRARY()

SRCS(
    cache.cpp
    cache_base.cpp
    rpc.cpp
)

PEERDIR(
    yt/yt/core
    yt/yt/client
    yt/yt_proto/yt/client/cache
)

END()

RECURSE_FOR_TESTS(
    unittests
)
