LIBRARY()

SRCS(
    counters.cpp
    net.cpp
)

PEERDIR(
    library/cpp/ipv6_address
    library/cpp/monlib/dynamic_counters
    ydb/core/base
)

END()

RECURSE_FOR_TESTS(
    ut
)
