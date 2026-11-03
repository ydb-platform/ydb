LIBRARY()

SRCS(stats.cpp)

PEERDIR(
    library/cpp/time_provider
    ydb/core/protos
)

END()

RECURSE_FOR_TESTS(ut)
