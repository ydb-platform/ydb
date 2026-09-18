LIBRARY()

SRCS(
    dq_pattern_cache.cpp
)

PEERDIR(
    library/cpp/monlib/dynamic_counters
    library/cpp/threading/future
    yql/essentials/minikql/computation
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
