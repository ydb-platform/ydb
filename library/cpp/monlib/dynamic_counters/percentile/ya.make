LIBRARY()

    SRCS(
        percentile.h
        percentile_lg.h
    )

    PEERDIR(
        library/cpp/containers/stack_vector
        library/cpp/monlib/dynamic_counters
    )

END()

RECURSE_FOR_TESTS(
    ut
)
