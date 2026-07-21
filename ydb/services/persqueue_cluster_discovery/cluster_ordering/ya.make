LIBRARY()

SRCS(
    weighed_ordering.cpp
)

END()

RECURSE_FOR_TESTS(
    fuzz_targets
    ut
)
