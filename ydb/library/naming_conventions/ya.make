LIBRARY()

SRCS(
    naming_conventions.cpp
)

END()

RECURSE_FOR_TESTS(
    fuzz_targets
    ut
)
