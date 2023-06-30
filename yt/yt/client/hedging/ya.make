LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    cache.cpp
    config.cpp
    counter.cpp
    hedging.cpp
    hedging_executor.cpp
    logger.cpp
    options.cpp
    penalty_provider.cpp
    rpc.cpp
)

PEERDIR(
    yt/yt/core
    yt/yt/client
    yt/yt/library/profiling
    yt/yt_proto/yt/client/hedging

    library/cpp/iterator
)

END()

RECURSE_FOR_TESTS(unittests)
