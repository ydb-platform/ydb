LIBRARY()


SRCS(
    bucket_quoter.cpp
)

PEERDIR(
    library/cpp/deprecated/atomic
)

END()

RECURSE(
    fuzz_targets
)

RECURSE_FOR_TESTS(
    ut
)
