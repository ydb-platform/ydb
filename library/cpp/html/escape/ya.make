LIBRARY()


SRCS(
    escape.cpp
)

END()

RECURSE(
    fuzz_targets
)

RECURSE_FOR_TESTS(
    ut
)
