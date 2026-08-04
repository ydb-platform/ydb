LIBRARY()


SRCS(
    escape.cpp
)

END()

RECURSE_FOR_TESTS(
    fuzz_targets
    ut
)
