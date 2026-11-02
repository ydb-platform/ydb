LIBRARY()

SRCS(
    path_normalizer.cpp
)

PEERDIR(
    library/cpp/monlib/dynamic_counters
    ydb/core/base
    ydb/core/protos
)

END()

RECURSE_FOR_TESTS(
    ut
)
