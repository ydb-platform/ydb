LIBRARY()

SRCS(
    time_series_vec.h
)

END()

RECURSE_FOR_TESTS(
    fuzz_targets
    ut
)
